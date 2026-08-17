#!/usr/bin/env python3
# Real bitcoin rich list, built entirely from our own node. No third party,
# no estimates, no placeholder rows.
#
# Core's `dumptxoutset` writes a serialized snapshot of the whole UTXO set.
# Since v29 that file carries a header ("utxo\xff", version 2, network magic,
# base blockhash, coin count) and the coins are grouped by txid. Inside a
# group the count and each vout index are CompactSize, while the coin body
# (height/coinbase code, compressed amount, compressed script) uses Bitcoin's
# VARINT. That mix was confirmed against a throwaway regtest node before this
# script was written, not assumed.
#
# Memory discipline: 166 million UTXOs never live in Python. The parser
# streams the file and pipes one line per output straight into an UNLOGGED
# staging table with COPY, then SQL does the GROUP BY and the top-N sort.
# Peak Python memory is a few megabytes of buffer.
#
# Integrity gate: the parsed coin count and satoshi total must match the
# node's own coinstatsindex figures for the snapshot height before anything
# is written. A desynced parse can therefore never reach the public table.
#
# Run from the rich-list.timer systemd timer (daily in production; the box
# runs no cron daemon); needs to invoke bitcoin-cli as the
# bitcoin user (dumptxoutset is not on the web RPC whitelist) and read access
# to /etc/openbitcoin/*.env.
#
# Usage:
#   build-rich-list.py                  full run: dump, parse, publish, delete
#   build-rich-list.py --snapshot PATH  reuse an existing snapshot file
#   build-rich-list.py --dry-run        parse and rank, but do not publish
#   build-rich-list.py --keep           leave the snapshot file behind
import argparse
import base64
import fcntl
import glob
import json
import os
import pwd
import shutil
import subprocess
import sys
import time
import urllib.request
from binascii import hexlify
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2

SNAP_DIR = '/var/lib/openbitcoin/utxo'
LOCK_PATH = '/var/lock/openbitcoin-rich-list.lock'
TOP_N = 10000
STAGE = 'rich_list_stage'
AGG = 'rich_list_agg'
MIN_FREE_BYTES = 40 * 1024 ** 3   # the snapshot alone is ~11GB, staging ~16GB
DUMP_TIMEOUT = 4 * 3600
READ_CHUNK = 1 << 22              # 4 MiB reads from the snapshot
COIN_HEADROOM = 1 << 16           # refill before a coin could run off the end
COPY_CHUNK = 1 << 22              # bytes handed to COPY at a time


def load_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                env[k] = v
    return env


RPC = load_env('/etc/openbitcoin/rpc-obweb.env')
DB = load_env('/etc/openbitcoin/db.env')
RPC_URL = f"http://{RPC.get('RPC_HOST', '127.0.0.1')}:{RPC.get('RPC_PORT', '8332')}/"
AUTH_HDR = 'Basic ' + base64.b64encode(
    (RPC['OBWEB_RPC_USER'] + ':' + RPC['OBWEB_RPC_PASSWORD']).encode()).decode()


def rpc(method, params=None):
    body = json.dumps({'jsonrpc': '1.0', 'id': 'rl', 'method': method,
                       'params': params or []}).encode()
    req = urllib.request.Request(RPC_URL, data=body, headers={
        'Content-Type': 'application/json', 'Authorization': AUTH_HDR})
    with urllib.request.urlopen(req, timeout=30) as r:
        out = json.loads(r.read())
    if out.get('error'):
        raise RuntimeError(f"{method}: {out['error']}")
    return out['result']


def cli(args, timeout=120, parse_float=None):
    """bitcoin-cli as the bitcoin user: dumptxoutset and gettxoutsetinfo are
    deliberately absent from the obweb RPC whitelist."""
    cmd = ['/usr/local/bin/bitcoin-cli'] + args
    try:
        if os.geteuid() != pwd.getpwnam('bitcoin').pw_uid:
            cmd = ['/usr/bin/sudo', '-n', '-u', 'bitcoin'] + cmd
    except KeyError:
        raise RuntimeError('no bitcoin user on this box')
    p = subprocess.run(cmd, capture_output=True, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"bitcoin-cli {args[-1]}: {p.stderr.decode().strip()[:300]}")
    out = p.stdout.decode().strip()
    try:
        return json.loads(out, parse_float=parse_float)
    except json.JSONDecodeError:
        return out


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


# ---- address encoding -------------------------------------------------------
# Chain parameters keyed by the network magic in the snapshot header, so the
# same parser produces correct addresses on regtest (where it was verified
# against listunspent) and on mainnet.
NETWORKS = {
    'f9beb4d9': ('mainnet', 'bc', 0x00, 0x05),
    '0b110907': ('testnet3', 'tb', 0x6f, 0xc4),
    '1c163f28': ('testnet4', 'tb', 0x6f, 0xc4),
    '0a03cf40': ('signet', 'tb', 0x6f, 0xc4),
    'fabfb5da': ('regtest', 'bcrt', 0x6f, 0xc4),
}

B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
BECH32 = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l'


def b58check(payload):
    import hashlib
    chk = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    n = int.from_bytes(payload + chk, 'big')
    out = ''
    while n > 0:
        n, r = divmod(n, 58)
        out = B58[r] + out
    for b in payload:
        if b:
            break
        out = '1' + out
    return out


def _bech32_polymod(values):
    gen = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
    chk = 1
    for v in values:
        top = chk >> 25
        chk = ((chk & 0x1ffffff) << 5) ^ v
        for i in range(5):
            chk ^= gen[i] if ((top >> i) & 1) else 0
    return chk


def _convertbits(data, frm, to, pad=True):
    acc = 0
    bits = 0
    out = []
    maxv = (1 << to) - 1
    for value in data:
        acc = (acc << frm) | value
        bits += frm
        while bits >= to:
            bits -= to
            out.append((acc >> bits) & maxv)
    if pad and bits:
        out.append((acc << (to - bits)) & maxv)
    return out


def bech32_address(hrp, witver, prog):
    const = 1 if witver == 0 else 0x2bc830a3
    data = [witver] + _convertbits(prog, 8, 5)
    values = [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp] + data
    polymod = _bech32_polymod(values + [0, 0, 0, 0, 0, 0]) ^ const
    checksum = [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]
    return hrp + '1' + ''.join(BECH32[d] for d in data + checksum)


def key_to_address(key, net):
    """Staging keys are compact on purpose: one type letter plus the hex
    payload, so 166 million rows never carry a base58 or bech32 string. Only
    the ranked few thousand are ever encoded."""
    hrp, p2pkh_ver, p2sh_ver = net[1], net[2], net[3]
    t, payload = key[0], bytes.fromhex(key[1:])
    if t == 'p':
        return b58check(bytes([p2pkh_ver]) + payload)
    if t == 's':
        return b58check(bytes([p2sh_ver]) + payload)
    return bech32_address(hrp, ord(t) - 97, payload)


# ---- snapshot parsing -------------------------------------------------------
def read_header(f):
    hdr = f.read(51)
    if len(hdr) < 51 or hdr[:5] != b'utxo\xff':
        raise RuntimeError('not a Core UTXO snapshot: bad magic')
    version = int.from_bytes(hdr[5:7], 'little')
    if version != 2:
        raise RuntimeError(f'unsupported snapshot version {version}; parser targets v2')
    magic = hdr[7:11].hex()
    if magic not in NETWORKS:
        raise RuntimeError(f'unknown network magic {magic}')
    return {
        'version': version,
        'net': NETWORKS[magic],
        'base_hash': hdr[11:43][::-1].hex(),
        'coins': int.from_bytes(hdr[43:51], 'little'),
    }


def decompress_amount(x):
    if x == 0:
        return 0
    x -= 1
    e = x % 10
    x //= 10
    if e < 9:
        d = (x % 9) + 1
        x //= 9
        n = x * 10 + d
    else:
        n = x + 1
    return n * 10 ** e


def feed_rows(f, header, stats):
    """Generator of COPY-ready bytes: one 'key\tsats\theight' line per output
    that resolves to an address. Address-less scripts (bare pubkey, bare
    multisig, OP_RETURN, anything non standard) are counted and left out of
    the ranking rather than invented."""
    total_coins = header['coins']
    buf = f.read(READ_CHUNK)
    pos = 0
    seen = 0
    total_sat = 0
    skipped = 0
    skipped_sat = 0
    out = []
    out_bytes = 0
    started = time.time()
    while seen < total_coins:
        if len(buf) - pos < COIN_HEADROOM:
            buf = buf[pos:] + f.read(READ_CHUNK)
            pos = 0
            if not buf:
                raise RuntimeError(f'snapshot truncated after {seen} of {total_coins} coins')
        pos += 32                                   # txid, not needed for balances
        # CompactSize: number of unspent outputs of this transaction
        c = buf[pos]
        pos += 1
        if c < 253:
            group = c
        elif c == 253:
            group = int.from_bytes(buf[pos:pos + 2], 'little'); pos += 2
        elif c == 254:
            group = int.from_bytes(buf[pos:pos + 4], 'little'); pos += 4
        else:
            group = int.from_bytes(buf[pos:pos + 8], 'little'); pos += 8
        for _ in range(group):
            if len(buf) - pos < COIN_HEADROOM:
                buf = buf[pos:] + f.read(READ_CHUNK)
                pos = 0
            # CompactSize vout index
            c = buf[pos]
            pos += 1
            if c == 253:
                pos += 2
            elif c == 254:
                pos += 4
            elif c == 255:
                pos += 8
            # VARINT height*2 + coinbase flag
            n = 0
            while True:
                c = buf[pos]
                pos += 1
                n = (n << 7) | (c & 0x7f)
                if c & 0x80:
                    n += 1
                else:
                    break
            height = n >> 1
            # VARINT compressed amount
            n = 0
            while True:
                c = buf[pos]
                pos += 1
                n = (n << 7) | (c & 0x7f)
                if c & 0x80:
                    n += 1
                else:
                    break
            sats = decompress_amount(n)
            # VARINT script size, with Core's six special compressed forms
            n = 0
            while True:
                c = buf[pos]
                pos += 1
                n = (n << 7) | (c & 0x7f)
                if c & 0x80:
                    n += 1
                else:
                    break
            key = None
            if n == 0:                              # P2PKH, 20 byte hash160
                key = b'p' + hexlify(buf[pos:pos + 20])
                pos += 20
            elif n == 1:                            # P2SH, 20 byte hash160
                key = b's' + hexlify(buf[pos:pos + 20])
                pos += 20
            elif n < 6:                             # bare pubkey, no address
                pos += 32
            else:
                ln = n - 6
                if pos + ln > len(buf):
                    buf = buf[pos:] + f.read(max(READ_CHUNK, ln + 64))
                    pos = 0
                spk = buf[pos:pos + ln]
                pos += ln
                if ln == 22 and spk[0] == 0x00 and spk[1] == 0x14:
                    key = b'a' + hexlify(spk[2:])           # P2WPKH
                elif ln == 34 and spk[0] == 0x00 and spk[1] == 0x20:
                    key = b'a' + hexlify(spk[2:])           # P2WSH
                elif ln == 34 and spk[0] == 0x51 and spk[1] == 0x20:
                    key = b'b' + hexlify(spk[2:])           # P2TR
                elif 4 <= ln <= 42 and spk[1] == ln - 2 and 0x51 <= spk[0] <= 0x60:
                    key = bytes([97 + spk[0] - 0x50]) + hexlify(spk[2:])  # witness v2-v16
            seen += 1
            total_sat += sats
            if key is None:
                skipped += 1
                skipped_sat += sats
                continue
            line = b'%s\t%d\t%d\n' % (key, sats, height)
            out.append(line)
            out_bytes += len(line)
            if out_bytes >= COPY_CHUNK:
                yield b''.join(out)
                out = []
                out_bytes = 0
        if seen % 20_000_000 < group and seen:
            rate = seen / max(1e-9, time.time() - started)
            log(f'  parsed {seen:,} of {total_coins:,} coins ({rate/1000:.0f}k/s)')
    if out:
        yield b''.join(out)
    stats.update(coins=seen, total_sat=total_sat, skipped=skipped,
                 skipped_sat=skipped_sat, parse_secs=time.time() - started)


class ChunkReader:
    """File-like adapter so psycopg2's COPY pulls straight from the parser."""

    def __init__(self, gen):
        self._gen = gen
        self._buf = b''
        self._done = False

    def _fill(self, size):
        while len(self._buf) < size and not self._done:
            try:
                self._buf += next(self._gen)
            except StopIteration:
                self._done = True

    def read(self, size=-1):
        if size is None or size < 0:
            self._fill(1 << 62)
            out, self._buf = self._buf, b''
            return out
        self._fill(size)
        out, self._buf = self._buf[:size], self._buf[size:]
        return out

    def readline(self, size=-1):
        while b'\n' not in self._buf and not self._done:
            self._fill(len(self._buf) + (1 << 20))
        nl = self._buf.find(b'\n')
        if nl < 0:
            out, self._buf = self._buf, b''
            return out
        out, self._buf = self._buf[:nl + 1], self._buf[nl + 1:]
        return out


# ---- main -------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description='build the rich list from a UTXO snapshot')
    ap.add_argument('--snapshot', help='reuse this snapshot file instead of dumping a new one')
    ap.add_argument('--keep', action='store_true', help='do not delete the snapshot file')
    ap.add_argument('--dry-run', action='store_true', help='rank but do not publish')
    ap.add_argument('--top', type=int, default=TOP_N)
    args = ap.parse_args()

    lock = open(LOCK_PATH, 'w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise RuntimeError('another rich-list run holds the lock; aborting')

    snap_path = args.snapshot
    dumped = False
    conn = None
    t0 = time.time()
    try:
        if not snap_path:
            os.makedirs(SNAP_DIR, exist_ok=True)
            free = shutil.disk_usage(SNAP_DIR).free
            if free < MIN_FREE_BYTES:
                raise RuntimeError(f'only {free/1e9:.0f}GB free, need {MIN_FREE_BYTES/1e9:.0f}GB')
            snap_path = os.path.join(SNAP_DIR, 'utxo-snapshot.dat')
            if os.path.exists(snap_path):
                os.unlink(snap_path)
            # bitcoind writes the file itself, so the directory has to be
            # writable by the bitcoin user
            shutil.chown(SNAP_DIR, 'bitcoin', 'bitcoin')
            os.chmod(SNAP_DIR, 0o755)
            log('dumptxoutset starting (several minutes, tens of GB)')
            t = time.time()
            res = cli(['-rpcclienttimeout=0', 'dumptxoutset', snap_path, 'latest'],
                      timeout=DUMP_TIMEOUT)
            dumped = True
            log(f"dumptxoutset done in {time.time()-t:.0f}s: {res['coins_written']:,} coins "
                f"at height {res['base_height']}, {os.path.getsize(snap_path)/1e9:.1f}GB")

        with open(snap_path, 'rb') as f:
            header = read_header(f)
            net = header['net']
            log(f"snapshot v{header['version']} {net[0]} base {header['base_hash'][:16]} "
                f"{header['coins']:,} coins")
            hdr_info = rpc('getblockheader', [header['base_hash']])
            height = hdr_info['height']
            snap_time = datetime.fromtimestamp(hdr_info['time'], timezone.utc)

            # the node's own numbers for this height, straight from
            # coinstatsindex: the parse has to agree with them exactly
            truth = cli(['gettxoutsetinfo', 'none', str(height)], parse_float=Decimal)
            truth_sat = int(truth['total_amount'] * 100000000)
            truth_outs = int(truth['txouts'])
            if truth_outs != header['coins']:
                raise RuntimeError(f'node says {truth_outs} outputs, snapshot header says {header["coins"]}')

            conn = psycopg2.connect(host=DB['PGHOST'], port=DB['PGPORT'], dbname=DB['PGDATABASE'],
                                    user=DB['PGUSER'], password=DB['PGPASSWORD'])
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SET work_mem = '1GB'")
            cur.execute("SET maintenance_work_mem = '1GB'")
            cur.execute('SET synchronous_commit = off')
            cur.execute(f'DROP TABLE IF EXISTS {STAGE}')
            cur.execute(f'CREATE UNLOGGED TABLE {STAGE} (k text NOT NULL, v bigint NOT NULL, h integer NOT NULL)')

            stats = {}
            log('streaming outputs into staging (COPY)')
            t = time.time()
            cur.copy_expert(f'COPY {STAGE} (k, v, h) FROM STDIN',
                            ChunkReader(feed_rows(f, header, stats)))
            copy_secs = time.time() - t
            tail = f.read(1)

        if not stats:
            raise RuntimeError('the parser never reached the end of the snapshot')
        if tail:
            raise RuntimeError('snapshot has trailing bytes: parse desynced')
        if stats['coins'] != header['coins']:
            raise RuntimeError(f"parsed {stats['coins']} coins, header promised {header['coins']}")
        if stats['total_sat'] != truth_sat:
            raise RuntimeError(f"parsed {stats['total_sat']} sat, node says {truth_sat} sat")
        log(f"parse verified against the node: {stats['coins']:,} coins, "
            f"{stats['total_sat']/1e8:,.8f} BTC, parse {stats['parse_secs']:.0f}s, copy {copy_secs:.0f}s")
        log(f"address-less scripts held out of the ranking: {stats['skipped']:,} outputs, "
            f"{stats['skipped_sat']/1e8:,.2f} BTC")

        log('aggregating per address in SQL')
        t = time.time()
        cur.execute(f'DROP TABLE IF EXISTS {AGG}')
        cur.execute(f"""
            CREATE UNLOGGED TABLE {AGG} AS
            SELECT k, sum(v)::bigint AS bal, count(*)::int AS n, min(h) AS first_h
            FROM {STAGE} GROUP BY k""")
        cur.execute(f'DROP TABLE IF EXISTS {STAGE}')     # ~16GB back immediately
        cur.execute(f'SELECT count(*) FROM {AGG}')
        distinct = cur.fetchone()[0]
        cur.execute(f'SELECT k, bal, n, first_h FROM {AGG} ORDER BY bal DESC LIMIT %s', (args.top,))
        top = cur.fetchall()
        log(f'aggregated in {time.time()-t:.0f}s: {distinct:,} addresses hold a balance, '
            f'top {len(top):,} kept')

        # distribution buckets over every funded address, decade edges from
        # 0.00001 BTC to 1M BTC in sats; bucket 0 is dust below 1,000 sats
        log('bucketing every balance for the distribution table')
        t = time.time()
        dist_edges = [10**e for e in range(3, 15)]
        cur.execute(f'''
            SELECT width_bucket(bal, %s::bigint[]) AS b, count(*)::bigint, sum(bal)::bigint
            FROM {AGG} WHERE bal > 0 GROUP BY b ORDER BY b''', (dist_edges,))
        dist = cur.fetchall()
        log(f'bucketed in {time.time()-t:.0f}s: {len(dist)} occupied buckets')

        # block dates for the oldest coin each ranked address still holds.
        # Paced: this loop fires two RPCs per height across thousands of
        # heights; throttle it so it never crowds out concurrent lookups
        dates = {}
        heights = sorted({r[3] for r in top})
        for h in heights:
            dates[h] = datetime.fromtimestamp(
                rpc('getblockheader', [rpc('getblockhash', [h])])['time'], timezone.utc).date()
            time.sleep(0.02)

        rows = []
        for i, (k, bal, n, first_h) in enumerate(top, 1):
            rows.append((i, key_to_address(k, net), bal, n, dates[first_h], height, snap_time))

        log('top 10 by balance:')
        for r in rows[:10]:
            log(f'  {r[0]:>3}. {r[1]:<64} {r[2]/1e8:>16,.8f} BTC  {r[3]:>8,} utxos')

        if args.dry_run:
            log('dry run: rich_list left untouched')
            return

        cur.execute("""
            CREATE TABLE IF NOT EXISTS rich_list (
              rank integer PRIMARY KEY,
              address text NOT NULL,
              balance_sat bigint NOT NULL,
              utxo_count integer NOT NULL,
              first_seen date,
              snapshot_height integer NOT NULL,
              snapshot_time timestamptz NOT NULL
            )""")
        cur.execute("""COMMENT ON COLUMN rich_list.first_seen IS
            'block date of the oldest coin the address still holds, which is not the same as the address first ever appearing on chain'""")
        # one transaction, so a reader either sees yesterday's list or today's
        conn.autocommit = False
        cur.execute('DELETE FROM rich_list')
        cur.executemany(
            """INSERT INTO rich_list (rank, address, balance_sat, utxo_count, first_seen,
                                      snapshot_height, snapshot_time)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""", rows)
        # the distribution swaps in the same transaction as the ranks
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rich_distribution (
              bucket integer PRIMARY KEY,
              floor_sat bigint NOT NULL,
              addresses bigint NOT NULL,
              balance_sat bigint NOT NULL,
              snapshot_height integer NOT NULL,
              snapshot_time timestamptz NOT NULL)""")
        cur.execute('DELETE FROM rich_distribution')
        dist_floors = [0] + dist_edges
        cur.executemany(
            """INSERT INTO rich_distribution (bucket, floor_sat, addresses, balance_sat,
                                             snapshot_height, snapshot_time)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            [(int(b), dist_floors[int(b)], int(n), int(v), height, snap_time)
             for (b, n, v) in dist])
        # byproduct for the footer and /stats: the sweep already
        # knows how many addresses hold a balance AND the exact circulating
        # supply (total_sat is verified against gettxoutsetinfo above), so
        # persist both; the data service adds the issuance since this height
        # to keep circulation current between sweeps.
        # raw blockchain size = the blk*.dat files only, the figure the world
        # quotes as "blockchain size"; the undo data (rev*.dat) that
        # size_on_disk also counts is ~106 GB the node keeps for reorgs
        raw_bytes = sum(os.stat(p).st_size
                        for p in glob.glob('/home/bitcoin/.bitcoin/blocks/blk*.dat'))
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chain_stats (
              id integer PRIMARY KEY,
              funded_addresses bigint NOT NULL,
              supply_sat bigint,
              raw_blocks_bytes bigint,
              raw_height integer,
              snapshot_height integer NOT NULL,
              snapshot_time timestamptz NOT NULL)""")
        cur.execute('ALTER TABLE chain_stats ADD COLUMN IF NOT EXISTS supply_sat bigint')
        cur.execute('ALTER TABLE chain_stats ADD COLUMN IF NOT EXISTS raw_blocks_bytes bigint')
        cur.execute('ALTER TABLE chain_stats ADD COLUMN IF NOT EXISTS raw_height integer')
        cur.execute("""INSERT INTO chain_stats (id, funded_addresses, supply_sat, raw_blocks_bytes, raw_height, snapshot_height, snapshot_time)
                       VALUES (1, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (id) DO UPDATE SET funded_addresses = EXCLUDED.funded_addresses,
                         supply_sat = EXCLUDED.supply_sat,
                         raw_blocks_bytes = EXCLUDED.raw_blocks_bytes,
                         raw_height = EXCLUDED.raw_height,
                         snapshot_height = EXCLUDED.snapshot_height,
                         snapshot_time = EXCLUDED.snapshot_time""",
                    (distinct, stats['total_sat'], raw_bytes, height, height, snap_time))
        cur.execute("""INSERT INTO job_heartbeats (job, last_success, detail)
                       VALUES ('rich-list', now(), %s)
                       ON CONFLICT (job) DO UPDATE SET last_success = now(), detail = EXCLUDED.detail""",
                    (f"height {height}, {len(rows)} rows, {distinct} addresses with balance, "
                     f"{stats['total_sat']/1e8:,.2f} BTC in {stats['coins']} utxos",))
        conn.commit()
        conn.autocommit = True
        log(f'published {len(rows)} rows at height {height} in {time.time()-t0:.0f}s total')

    finally:
        try:
            if conn:
                conn.rollback()
                conn.autocommit = True
                c = conn.cursor()
                c.execute(f'DROP TABLE IF EXISTS {STAGE}')
                c.execute(f'DROP TABLE IF EXISTS {AGG}')
                conn.close()
        except Exception as e:
            print(f'staging cleanup failed: {e}', file=sys.stderr)
        if dumped and not args.keep and snap_path and os.path.exists(snap_path):
            os.unlink(snap_path)
            log('snapshot file deleted')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'rich-list FAILED: {type(e).__name__}: {e}', file=sys.stderr)
        sys.exit(1)
