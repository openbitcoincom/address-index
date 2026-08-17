#!/usr/bin/env python3
# A complete address index for openbitcoin.com: lifetime received, lifetime
# sent, transaction count and activity span for every address that has ever
# appeared in a standard output on mainnet. Built entirely from our own node.
#
# Why no UTXO map of our own is needed
# ------------------------------------
# Core v31's `getblock <hash> 3` returns every input with its `prevout`
# inline, address and value included. One forward pass over the chain
# therefore sees both sides of every transaction. That was verified against
# this node before the script was written, not assumed: block 800000 input 0
# came back with prevout.scriptPubKey.address populated.
#
# What counts as an address
# -------------------------
# Only what Core itself names in scriptPubKey.address. Bare pubkey (P2PK),
# bare multisig, OP_RETURN and non standard scripts have no address and are
# left out rather than invented. That is deliberate: it matches what Fulcrum
# indexes, and Fulcrum is what the site's own address route reports from. The
# genesis coinbase is a P2PK output, so 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa
# begins at height 123723 here exactly as it does on the live site.
#
# Memory discipline
# -----------------
# Roughly 5.7 billion address touches never live in Python. Each worker holds
# one block, folds it into a per address dict for that block alone, and hands
# back compact text lines. The parent buffers a few hundred megabytes at most,
# gzips each buffer and appends it to one of 256 bucket files. Because a
# bucket is chosen by crc32 of the address, every row for a given address
# lands in exactly one bucket, so each bucket can later be aggregated on its
# own with a plain GROUP BY and appended to the output table. No global sort,
# no random upserts into a billion row btree, no unbounded Python dict.
#
# Resumability
# ------------
# The scan checkpoints the completed height together with the exact byte
# length of all 256 bucket files. An interrupted build truncates the files
# back to those lengths and carries on from the next block, so a restart
# never double counts. The fold phase records each finished bucket in the
# database inside the same transaction that appends its rows.
#
# Politeness
# ----------
# An electrum server (Fulcrum, in production) may share the box and has to
# keep serving. The scan runs niced, uses a modest number of RPC workers,
# and watches the
# node's own getblockcount latency: when it climbs the scan pauses and backs
# off until the node is comfortable again.
#
# Usage:
#   build-address-index.py --full            full build (resumes if interrupted)
#   build-address-index.py --incremental     apply new blocks (the forever mode)
#   build-address-index.py --verify          check sample addresses against the site
#   build-address-index.py --status          print where the index has got to
#   build-address-index.py --bench 200       time a short scan without writing
import argparse
import base64
import fcntl
import gzip
import http.client
import json
import os
import queue
import shutil
import signal
import sys
import threading
import time
import zlib
from collections import deque
from datetime import datetime, timezone
from multiprocessing import Pool

import psycopg2

WORK_DIR = '/var/lib/openbitcoin/addridx'
LOCK_PATH = '/var/lock/openbitcoin-address-index.lock'
STATE_PATH = os.path.join(WORK_DIR, 'scan-state.json')
BUCKETS = 256
STAGE_PREFIX = 'address_index_stage'
BUILD_TABLE = 'address_stats_build'
LIVE_TABLE = 'address_stats'
META_TABLE = 'address_index_meta'
FOLD_TABLE = 'address_index_fold'

# Index to the tip: visitors should see totals that
# include the block that just arrived, not two behind. The safety mechanism
# for reorgs was never this lag; it is the hash check and explicit undo in
# run_incremental, which keeps a 20 block undo window against reorgs that in
# practice reach 2 or 3 at the very worst. The lag only made that code path
# rare, at the cost of a permanent two block staleness. Unconfirmed
# transactions never enter the index at all, so a dropped or double spent
# mempool tx has nothing here to unwind.
CONFIRM_LAG = 0

FLUSH_BYTES = 512 * 1024          # per bucket buffer before it is gzipped out
CHECKPOINT_BLOCKS = 2000          # blocks between durable scan checkpoints
INCREMENTAL_MAX_BLOCKS = 240      # per run, so one catch up cannot balloon
LATENCY_SAMPLE_BLOCKS = 250       # how often the node is asked how it feels
LATENCY_LIMIT = 0.75              # seconds for getblockcount before backing off
LATENCY_RELIEF = 0.25             # seconds it must fall back under to resume


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
RPC_HOST = RPC.get('RPC_HOST', '127.0.0.1')
RPC_PORT = int(RPC.get('RPC_PORT', '8332'))
AUTH_HDR = 'Basic ' + base64.b64encode(
    (RPC['OBWEB_RPC_USER'] + ':' + RPC['OBWEB_RPC_PASSWORD']).encode()).decode()


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


# ---- RPC --------------------------------------------------------------------
# One keep alive connection per process. Nearly two million calls over a full
# build is a lot of TCP handshakes to skip.
_conn = threading.local()


def _http():
    # keyed on the pid as well as the thread: a forked worker inherits the
    # parent's open socket, and several processes talking over one connection
    # interleave their requests into nonsense
    c = getattr(_conn, 'c', None)
    if c is None or getattr(_conn, 'pid', None) != os.getpid():
        if c is not None:
            try:
                c.close()
            except Exception:
                pass
        c = http.client.HTTPConnection(RPC_HOST, RPC_PORT, timeout=180)
        _conn.c = c
        _conn.pid = os.getpid()
    return c


def rpc(method, params=None, retries=4):
    body = json.dumps({'jsonrpc': '1.0', 'id': 'ai', 'method': method,
                       'params': params or []})
    headers = {'Content-Type': 'application/json', 'Authorization': AUTH_HDR}
    last = None
    for attempt in range(retries):
        try:
            c = _http()
            c.request('POST', '/', body=body, headers=headers)
            resp = c.getresponse()
            data = resp.read()
            if resp.status != 200:
                # Core answers 500 with a JSON error body for real RPC errors
                try:
                    err = json.loads(data).get('error')
                except Exception:
                    err = data[:200]
                raise RuntimeError(f'{method}: HTTP {resp.status} {err}')
            out = json.loads(data)
            if out.get('error'):
                raise RuntimeError(f"{method}: {out['error']}")
            return out['result']
        except (http.client.HTTPException, OSError, ValueError) as e:
            # a dropped keep alive connection is normal after an idle spell
            last = e
            try:
                if getattr(_conn, 'c', None):
                    _conn.c.close()
            except Exception:
                pass
            _conn.c = None
            if attempt + 1 < retries:
                time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f'{method} failed after {retries} tries: {last}')


def sats(v):
    """Core reports amounts as JSON numbers with eight decimals. Every real
    output value is far inside the range where a double round trips exactly,
    so rounding after scaling recovers the satoshi integer. The verification
    gate against Fulcrum's own totals is what proves it, not this comment."""
    return int(round(v * 1e8))


# ---- per block folding ------------------------------------------------------
def fold_block(height, sign=1):
    """Return (height, {bucket: bytes}) for one block.

    Rows are `address, received, sent, tx_count, height`, already folded within
    the block so an address touched by several outputs of the same transaction
    produces one row, and tx_count counts each transaction once per address
    even when the address is on both sides of it."""
    blk = rpc('getblock', [rpc('getblockhash', [height]), 3])
    acc = {}
    for ti, tx in enumerate(blk['tx']):
        for o in tx['vout']:
            a = o['scriptPubKey'].get('address')
            if a is None:
                continue
            e = acc.get(a)
            if e is None:
                acc[a] = [sats(o['value']), 0, 1, ti]
            else:
                e[0] += sats(o['value'])
                if e[3] != ti:
                    e[2] += 1
                    e[3] = ti
        for i in tx['vin']:
            pv = i.get('prevout')
            if pv is None:            # coinbase
                continue
            a = pv['scriptPubKey'].get('address')
            if a is None:
                continue
            e = acc.get(a)
            if e is None:
                acc[a] = [0, sats(pv['value']), 1, ti]
            else:
                e[1] += sats(pv['value'])
                if e[3] != ti:
                    e[2] += 1
                    e[3] = ti
    parts = [None] * BUCKETS
    crc = zlib.crc32
    for a, e in acc.items():
        ab = a.encode()
        b = crc(ab) & (BUCKETS - 1)
        line = b'%s\t%d\t%d\t%d\t%d\n' % (ab, sign * e[0], sign * e[1], sign * e[2], height)
        if parts[b] is None:
            parts[b] = [line]
        else:
            parts[b].append(line)
    return height, {b: b''.join(v) for b, v in enumerate(parts) if v is not None}


def _fold_json(blk, height, sign=1):
    """The same fold as fold_block but returned as a plain address dict, which
    is what the incremental path wants. sign=-1 negates every delta, which is
    how a reorged block is undone."""
    acc = {}
    for ti, tx in enumerate(blk['tx']):
        for o in tx['vout']:
            a = o['scriptPubKey'].get('address')
            if a is None:
                continue
            e = acc.get(a)
            if e is None:
                acc[a] = [sats(o['value']), 0, 1, ti]
            else:
                e[0] += sats(o['value'])
                if e[3] != ti:
                    e[2] += 1
                    e[3] = ti
        for i in tx['vin']:
            pv = i.get('prevout')
            if pv is None:
                continue
            a = pv['scriptPubKey'].get('address')
            if a is None:
                continue
            e = acc.get(a)
            if e is None:
                acc[a] = [0, sats(pv['value']), 1, ti]
            else:
                e[1] += sats(pv['value'])
                if e[3] != ti:
                    e[2] += 1
                    e[3] = ti
    if sign < 0:
        for e in acc.values():
            e[0] = -e[0]
            e[1] = -e[1]
            e[2] = -e[2]
    return acc


def _worker_init():
    try:
        os.nice(5)
    except OSError:
        pass
    _conn.c = None      # never reuse the socket we were forked with


def _worker_task(h):
    return fold_block(h)


# ---- database ---------------------------------------------------------------
def connect():
    c = psycopg2.connect(host=DB['PGHOST'], port=DB['PGPORT'], dbname=DB['PGDATABASE'],
                         user=DB['PGUSER'], password=DB['PGPASSWORD'])
    c.autocommit = True
    return c


def ensure_schema(cur, table=LIVE_TABLE):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
          address text PRIMARY KEY,
          received_sat bigint NOT NULL,
          sent_sat bigint NOT NULL,
          tx_count int NOT NULL,
          first_height int,
          last_height int,
          last_spend_height int,
          updated_at timestamptz
        )""")
    cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS last_spend_height int')
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {META_TABLE} (
          id int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
          indexed_height int NOT NULL,
          indexed_hash text,
          recent_hashes jsonb NOT NULL DEFAULT '[]'::jsonb,
          updated_at timestamptz NOT NULL DEFAULT now()
        )""")
    cur.execute(f"""COMMENT ON TABLE {META_TABLE} IS
        'the height the address index is complete through; a row in address_stats is only authoritative for activity at or below indexed_height'""")


def read_meta(cur):
    cur.execute(f'SELECT indexed_height, indexed_hash, recent_hashes FROM {META_TABLE} WHERE id = 1')
    r = cur.fetchone()
    if not r:
        return None
    return {'height': r[0], 'hash': r[1], 'recent': r[2] or []}


def write_meta(cur, height, bhash, recent):
    cur.execute(f"""
        INSERT INTO {META_TABLE} (id, indexed_height, indexed_hash, recent_hashes, updated_at)
        VALUES (1, %s, %s, %s, now())
        ON CONFLICT (id) DO UPDATE SET indexed_height = EXCLUDED.indexed_height,
          indexed_hash = EXCLUDED.indexed_hash, recent_hashes = EXCLUDED.recent_hashes,
          updated_at = now()""",
                (height, bhash, json.dumps(recent)))


def heartbeat(cur, detail):
    cur.execute("""INSERT INTO job_heartbeats (job, last_success, detail)
                   VALUES ('address-index', now(), %s)
                   ON CONFLICT (job) DO UPDATE SET last_success = now(), detail = EXCLUDED.detail""",
                (detail[:500],))


# ---- scan phase -------------------------------------------------------------
class BucketWriter:
    """256 append only files of concatenated gzip members. Members are self
    contained, so truncating a file at a member boundary leaves valid data,
    which is what makes the checkpoint and resume honest."""

    def __init__(self, directory):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        self.paths = [os.path.join(directory, f'b{b:03d}.gz') for b in range(BUCKETS)]
        self.files = [open(p, 'ab') for p in self.paths]
        self.bufs = [[] for _ in range(BUCKETS)]
        self.sizes = [0] * BUCKETS
        self.raw_bytes = 0
        self.written = 0

    def add(self, parts):
        for b, data in parts.items():
            self.bufs[b].append(data)
            self.sizes[b] += len(data)
            self.raw_bytes += len(data)
            if self.sizes[b] >= FLUSH_BYTES:
                self._flush(b)

    def _flush(self, b):
        if not self.sizes[b]:
            return
        blob = b''.join(self.bufs[b])
        self.bufs[b] = []
        self.sizes[b] = 0
        packed = gzip.compress(blob, 1)
        self.files[b].write(packed)
        self.written += len(packed)

    def flush_all(self):
        for b in range(BUCKETS):
            self._flush(b)
        for f in self.files:
            f.flush()
            os.fsync(f.fileno())

    def lengths(self):
        return [os.fstat(f.fileno()).st_size for f in self.files]

    def truncate_to(self, lengths):
        # never extend: truncate() past the end of a file would pad it with
        # zeros, and a bucket file that has already been folded and deleted is
        # legitimately shorter than the checkpoint remembers
        for f, n in zip(self.files, lengths):
            f.truncate(min(n, os.fstat(f.fileno()).st_size))
            f.flush()

    def close(self):
        for f in self.files:
            f.close()


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return None


def save_state(state):
    tmp = STATE_PATH + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_PATH)


class NodeWatch:
    """The node's own getblockcount latency is the signal that we are leaning
    too hard on it. Fulcrum queries the same RPC, so when this climbs the
    scan gets out of the way until it settles."""

    def __init__(self):
        self.paused_secs = 0.0
        self.pauses = 0

    def check(self):
        t = time.time()
        rpc('getblockcount')
        lat = time.time() - t
        if lat < LATENCY_LIMIT:
            return lat
        self.pauses += 1
        log(f'  node RPC latency {lat:.2f}s, pausing to let it breathe')
        waited = 0.0
        while waited < 120:
            time.sleep(5)
            waited += 5
            t = time.time()
            rpc('getblockcount')
            lat = time.time() - t
            if lat < LATENCY_RELIEF:
                break
        self.paused_secs += waited
        log(f'  resumed after {waited:.0f}s (latency now {lat:.2f}s)')
        return lat


def run_scan(target_height, workers, resume_ok=True):
    state = load_state() if resume_ok else None
    # decided before any file is opened: once the fold has started deleting
    # bucket files there is nothing left to resume into
    if state and state.get('target') == target_height and state['height'] >= target_height:
        log('scan already complete')
        return
    writer = BucketWriter(WORK_DIR)
    start = 0
    if state and state.get('target') == target_height:
        writer.truncate_to(state['lengths'])
        start = state['height'] + 1
        log(f"resuming scan at height {start} (checkpoint had {sum(state['lengths'])/1e9:.1f}GB written)")
    elif state:
        log(f"discarding a checkpoint for a different target ({state.get('target')} vs {target_height})")
        writer.truncate_to([0] * BUCKETS)
    log(f'scan {start}..{target_height} with {workers} workers')

    watch = NodeWatch()
    t0 = time.time()
    done = 0
    stop = {'flag': False}

    def _sigterm(signum, frame):
        stop['flag'] = True
        log('signal received, will checkpoint and stop')
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    pool = Pool(processes=workers, initializer=_worker_init)
    try:
        heights = iter(range(start, target_height + 1))
        pending = deque()
        exhausted = False
        last_h = start - 1
        last_log = time.time()
        while True:
            while not exhausted and len(pending) < workers * 3:
                try:
                    h = next(heights)
                except StopIteration:
                    exhausted = True
                    break
                pending.append(pool.apply_async(_worker_task, (h,)))
            if not pending:
                break
            h, parts = pending.popleft().get()
            writer.add(parts)
            last_h = h
            done += 1

            if done % LATENCY_SAMPLE_BLOCKS == 0:
                watch.check()
            if time.time() - last_log > 120:
                rate = done / max(1e-9, time.time() - t0)
                left = target_height - h
                log(f'  height {h} ({done:,} done, {rate:.1f} blk/s, '
                    f'{writer.raw_bytes/1e9:.1f}GB raw, {writer.written/1e9:.1f}GB on disk, '
                    f'eta {left/max(rate,1e-9)/3600:.1f}h)')
                last_log = time.time()
            if done % CHECKPOINT_BLOCKS == 0 or stop['flag']:
                # drain what is already in flight so the checkpoint is a clean
                # contiguous frontier, then make it durable
                while pending:
                    hh, pp = pending.popleft().get()
                    writer.add(pp)
                    last_h = hh
                    done += 1
                writer.flush_all()
                save_state({'target': target_height, 'height': last_h,
                            'lengths': writer.lengths()})
                free = shutil.disk_usage(WORK_DIR).free
                if free < 60 * 1024 ** 3:
                    raise RuntimeError(f'only {free/1e9:.0f}GB free, stopping before the disk fills')
                if stop['flag']:
                    log(f'checkpointed at height {last_h}; rerun --full to continue')
                    return
                if last_h >= target_height:
                    break
        writer.flush_all()
        save_state({'target': target_height, 'height': last_h, 'lengths': writer.lengths(),
                    'scan_complete': last_h >= target_height})
    finally:
        pool.terminate()
        pool.join()
        writer.close()
    dt = time.time() - t0
    log(f'scan complete: {done:,} blocks in {dt/3600:.2f}h ({done/max(dt,1e-9):.1f} blk/s), '
        f'{writer.raw_bytes/1e9:.1f}GB raw folded into {writer.written/1e9:.1f}GB of bucket files, '
        f'{watch.pauses} politeness pauses totalling {watch.paused_secs:.0f}s')


# ---- fold phase -------------------------------------------------------------
class GzChunks:
    """File like adapter over a bucket file so COPY pulls decompressed rows
    straight off disk. Python's gzip reader handles the concatenated members
    the writer produced as one continuous stream."""

    def __init__(self, path):
        self.f = gzip.open(path, 'rb')

    def read(self, size=-1):
        return self.f.read(size)

    def readline(self, size=-1):
        return self.f.readline(size)

    def close(self):
        self.f.close()


def fold_bucket(conn, bucket, slot):
    stage = f'{STAGE_PREFIX}_{slot}'
    path = os.path.join(WORK_DIR, f'b{bucket:03d}.gz')
    cur = conn.cursor()
    cur.execute("SET work_mem = '1500MB'")
    cur.execute('SET synchronous_commit = off')
    cur.execute(f'DROP TABLE IF EXISTS {stage}')
    cur.execute(f'CREATE UNLOGGED TABLE {stage} (a text NOT NULL, r bigint NOT NULL, '
                f's bigint NOT NULL, n int NOT NULL, h int NOT NULL)')
    t = time.time()
    if os.path.exists(path) and os.path.getsize(path) > 0:
        src = GzChunks(path)
        try:
            cur.copy_expert(f'COPY {stage} (a, r, s, n, h) FROM STDIN', src)
        finally:
            src.close()
    cur.execute(f'SELECT count(*) FROM {stage}')
    raw = cur.fetchone()[0]
    copy_secs = time.time() - t

    t = time.time()
    conn.autocommit = False
    cur.execute(f"""
        INSERT INTO {BUILD_TABLE} (address, received_sat, sent_sat, tx_count,
                                   first_height, last_height, last_spend_height, updated_at)
        SELECT a, sum(r)::bigint, sum(s)::bigint, sum(n)::int, min(h), max(h),
               max(h) FILTER (WHERE s > 0), now()
        FROM {stage} GROUP BY a""")
    inserted = cur.rowcount
    cur.execute(f'INSERT INTO {FOLD_TABLE} (bucket, rows_in, rows_out, done_at) '
                f'VALUES (%s, %s, %s, now())', (bucket, raw, inserted))
    conn.commit()
    conn.autocommit = True
    cur.execute(f'DROP TABLE IF EXISTS {stage}')
    # The bucket is aggregated and committed, so its file is dead weight.
    # Freeing it now rather than at the end keeps the peak disk footprint to
    # roughly one copy of the data: the bucket files shrink as the output
    # table grows, instead of both being at full size at the same moment.
    if os.path.exists(path):
        os.unlink(path)
    return raw, inserted, copy_secs, time.time() - t


def run_fold(threads):
    conn = connect()
    cur = conn.cursor()
    cur.execute(f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {FOLD_TABLE} (
          bucket int PRIMARY KEY, rows_in bigint, rows_out bigint, done_at timestamptz)""")
    # the build table carries no primary key while it loads: the index is
    # built once at the end from sorted input, which is far cheaper than
    # maintaining a btree across 1.5 billion inserts
    # autovacuum off while it loads: the table only ever gains rows here, and
    # letting autovacuum rescan a table on its way to a hundred gigabytes
    # buys nothing. It goes back on at publish.
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {BUILD_TABLE} (
          address text NOT NULL,
          received_sat bigint NOT NULL,
          sent_sat bigint NOT NULL,
          tx_count int NOT NULL,
          first_height int,
          last_height int,
          last_spend_height int,
          updated_at timestamptz
        ) WITH (autovacuum_enabled = false)""")
    cur.execute(f'SELECT bucket FROM {FOLD_TABLE}')
    have = {r[0] for r in cur.fetchall()}
    todo = [b for b in range(BUCKETS) if b not in have]
    if not todo:
        log('fold already complete for all buckets')
        return
    free = shutil.disk_usage(WORK_DIR).free
    log(f'folding {len(todo)} buckets with {threads} threads ({free/1e9:.0f}GB free)')
    q = queue.Queue()
    for b in todo:
        q.put(b)
    totals = {'raw': 0, 'out': 0, 'done': 0, 'err': None}
    lock = threading.Lock()
    t0 = time.time()

    def worker(slot):
        c = connect()
        try:
            while True:
                try:
                    b = q.get_nowait()
                except queue.Empty:
                    return
                raw, out, cs, gs = fold_bucket(c, b, slot)
                with lock:
                    totals['raw'] += raw
                    totals['out'] += out
                    totals['done'] += 1
                    n = totals['done']
                    el = time.time() - t0
                    log(f'  bucket {b:3d}: {raw:,} rows in, {out:,} addresses out '
                        f'(copy {cs:.0f}s, group {gs:.0f}s) [{n}/{len(todo)}, '
                        f'eta {el/n*(len(todo)-n)/3600:.1f}h]')
        except Exception as e:
            with lock:
                totals['err'] = e
            log(f'fold thread {slot} failed: {type(e).__name__}: {e}')
        finally:
            c.close()

    ts = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    if totals['err']:
        raise totals['err']
    log(f"fold complete: {totals['raw']:,} rows aggregated into {totals['out']:,} addresses "
        f"in {(time.time()-t0)/3600:.2f}h")


def build_index_and_publish(target_height, target_hash, recent):
    conn = connect()
    cur = conn.cursor()
    cur.execute(f'SELECT count(*) FROM {FOLD_TABLE}')
    if cur.fetchone()[0] != BUCKETS:
        raise RuntimeError('not every bucket has been folded; refusing to publish')
    cur.execute(f'SELECT sum(rows_out) FROM {FOLD_TABLE}')
    expected = cur.fetchone()[0]
    cur.execute(f'SELECT count(*) FROM {BUILD_TABLE}')
    actual = cur.fetchone()[0]
    if expected != actual:
        raise RuntimeError(f'{BUILD_TABLE} holds {actual} rows, the fold log says {expected}')
    log(f'{actual:,} addresses in the build table, matching the fold log')

    cur.execute(f"SELECT to_regclass('{BUILD_TABLE}_pkey')")
    if cur.fetchone()[0] is None:
        log('building the primary key (one sorted pass, expect a while)')
        t = time.time()
        cur.execute("SET maintenance_work_mem = '6GB'")
        cur.execute(f'ALTER TABLE {BUILD_TABLE} ADD CONSTRAINT {BUILD_TABLE}_pkey PRIMARY KEY (address)')
        log(f'primary key built in {(time.time()-t)/60:.0f} min')
    cur.execute(f'ANALYZE {BUILD_TABLE}')

    cur.execute(f'ALTER TABLE {BUILD_TABLE} SET (autovacuum_enabled = true)')
    ensure_schema(cur, LIVE_TABLE)
    conn.autocommit = False
    cur.execute(f'DROP TABLE IF EXISTS {LIVE_TABLE}')
    cur.execute(f'ALTER TABLE {BUILD_TABLE} RENAME TO {LIVE_TABLE}')
    cur.execute(f'ALTER INDEX {BUILD_TABLE}_pkey RENAME TO {LIVE_TABLE}_pkey')
    write_meta(cur, target_height, target_hash, recent)
    heartbeat(cur, f'full build published: {actual} addresses through height {target_height}')
    conn.commit()
    conn.autocommit = True
    log(f'published {actual:,} addresses, index complete through height {target_height}')


# ---- incremental ------------------------------------------------------------
def apply_deltas(cur, acc):
    """One upsert for the whole batch. LEAST and GREATEST keep the activity
    span honest for addresses that already existed."""
    if not acc:
        return 0
    cur.execute('CREATE TEMP TABLE IF NOT EXISTS ai_delta (a text NOT NULL, r bigint NOT NULL, '
                's bigint NOT NULL, n int NOT NULL, fh int NOT NULL, lh int NOT NULL, '
                'ls int NOT NULL) ON COMMIT DROP')
    cur.execute('TRUNCATE ai_delta')
    buf = []
    for a, e in acc.items():
        buf.append('%s\t%d\t%d\t%d\t%d\t%d\t%d\n' % (a, e[0], e[1], e[2], e[3], e[4], e[5]))
    import io
    cur.copy_expert('COPY ai_delta (a, r, s, n, fh, lh, ls) FROM STDIN',
                    io.StringIO(''.join(buf)))
    cur.execute(f"""
        INSERT INTO {LIVE_TABLE} (address, received_sat, sent_sat, tx_count,
                                  first_height, last_height, last_spend_height, updated_at)
        SELECT a, sum(r)::bigint, sum(s)::bigint, sum(n)::int, min(fh), max(lh),
               NULLIF(max(ls), 0), now()
        FROM ai_delta GROUP BY a
        ON CONFLICT (address) DO UPDATE SET
          received_sat = {LIVE_TABLE}.received_sat + EXCLUDED.received_sat,
          sent_sat = {LIVE_TABLE}.sent_sat + EXCLUDED.sent_sat,
          tx_count = {LIVE_TABLE}.tx_count + EXCLUDED.tx_count,
          first_height = LEAST({LIVE_TABLE}.first_height, EXCLUDED.first_height),
          last_height = GREATEST({LIVE_TABLE}.last_height, EXCLUDED.last_height),
          last_spend_height = NULLIF(GREATEST(COALESCE({LIVE_TABLE}.last_spend_height, 0),
                                              COALESCE(EXCLUDED.last_spend_height, 0)), 0),
          updated_at = now()""")
    return len(acc)


def merge_into(acc, block_acc, height):
    for a, e in block_acc.items():
        cur = acc.get(a)
        if cur is None:
            acc[a] = [e[0], e[1], e[2], height, height, height if e[1] > 0 else 0]
        else:
            cur[0] += e[0]
            cur[1] += e[1]
            cur[2] += e[2]
            if e[1] > 0 and height > cur[5]:
                cur[5] = height
            if height < cur[3]:
                cur[3] = height
            if height > cur[4]:
                cur[4] = height


def run_incremental():
    conn = connect()
    cur = conn.cursor()
    ensure_schema(cur)
    meta = read_meta(cur)
    if meta is None or meta['height'] < 0:
        log('no published index yet; incremental has nothing to extend')
        return
    tip = rpc('getblockcount')
    target = tip - CONFIRM_LAG
    have = meta['height']

    # reorg check: the hash we recorded for the tip of the index must still be
    # the chain's hash at that height
    recent = list(meta['recent'] or [])
    if meta['hash']:
        try:
            current = rpc('getblockhash', [have])
        except Exception:
            current = None
        if current and current != meta['hash']:
            log(f'reorg detected at height {have}: indexed {meta["hash"][:16]}, chain has {current[:16]}')
            undo = {}
            rolled = 0
            for entry in reversed(recent):
                h, bh = entry['height'], entry['hash']
                if h > have:
                    continue
                try:
                    live = rpc('getblockhash', [h])
                except Exception:
                    live = None
                if live == bh:
                    break
                blk = rpc('getblock', [bh, 3])       # Core keeps stale blocks on disk
                merge_into(undo, _fold_json(blk, h, sign=-1), h)
                rolled += 1
                have = h - 1
            if rolled:
                conn.autocommit = False
                apply_deltas(cur, undo)
                conn.commit()
                conn.autocommit = True
                log(f'undid {rolled} stale block(s); index rewound to height {have}. '
                    f'first_height and last_height for the affected addresses are '
                    f'approximate until they next move')
            recent = [e for e in recent if e['height'] <= have]

    if target <= have:
        heartbeat(cur, f'up to date at height {have} (tip {tip})')
        log(f'nothing to do: index at {have}, tip {tip}, target {target}')
        return

    end = min(target, have + INCREMENTAL_MAX_BLOCKS)
    t0 = time.time()
    acc = {}
    last_hash = None
    for h in range(have + 1, end + 1):
        bh = rpc('getblockhash', [h])
        blk = rpc('getblock', [bh, 3])
        merge_into(acc, _fold_json(blk, h), h)
        recent.append({'height': h, 'hash': bh})
        last_hash = bh
    recent = recent[-20:]
    conn.autocommit = False
    n = apply_deltas(cur, acc)
    write_meta(cur, end, last_hash, recent)
    heartbeat(cur, f'incremental to height {end} (tip {tip}), {end-have} block(s), {n} addresses touched')
    conn.commit()
    conn.autocommit = True
    log(f'applied {end-have} block(s) up to {end}: {n:,} addresses touched in {time.time()-t0:.1f}s '
        f'(tip {tip})')


# ---- verification -----------------------------------------------------------
SAMPLE = [
    '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa',
    '17SkEw2md5avVNyYgj6RiXuQKNwkXaxFyQ',
    'bc1qwj4fajtya5dhtm8te2c2kdyd9cwq0gt8645zde',
    '3D2oetdNuZUqQHPJmcMDDHYoqkyNVsFk9r',
    'bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97',
    '12tkqA9xSoowkzoERHMWNKsTey55YEBqkv',
    '1BgGZ9tcN4rm9KBzDn7KprQz87SZ26SAMH',
    'bc1p5d7rjq7g6rdk2yhzks9smlaqtedr4dekq08ge8ztwac72sfr9rusxg3297',
]


def api_address(addr, timeout=600):
    c = http.client.HTTPConnection('127.0.0.1', 8090, timeout=timeout)
    c.request('GET', f'/v1/address/{addr}')
    r = c.getresponse()
    data = json.loads(r.read())
    c.close()
    return data


def run_verify(extra=None):
    conn = connect()
    cur = conn.cursor()
    meta = read_meta(cur)
    if not meta:
        log('no meta row: nothing published yet')
        return False
    log(f"index is complete through height {meta['height']}")
    cur.execute('SELECT address FROM rich_list ORDER BY rank LIMIT 3')
    rich = [r[0] for r in cur.fetchall()]
    addrs = list(dict.fromkeys(SAMPLE + rich + list(extra or [])))

    ok = True
    log('')
    log(f"{'address':<64} {'index bal (BTC)':>18} {'site bal (BTC)':>18}  {'txs idx/site':>16}  verdict")
    for a in addrs:
        cur.execute(f'SELECT received_sat, sent_sat, tx_count, first_height, last_height '
                    f'FROM {LIVE_TABLE} WHERE address = %s', (a,))
        row = cur.fetchone()
        try:
            api = api_address(a)
        except Exception as e:
            log(f'{a:<64} site query failed: {e}')
            ok = False
            continue
        if not row:
            log(f'{a:<64} MISSING from the index')
            ok = False
            continue
        recv, sent, txc, fh, lh = row
        bal = recv - sent
        site_bal = api.get('confirmedSat')
        site_tx = api.get('txCount')
        last_act = (api.get('lastActivity') or {}).get('height')
        first_act = (api.get('firstSeen') or {}).get('height')
        note = ''
        good = True
        if last_act is not None and last_act > meta['height']:
            note = f'moved at {last_act}, past the index frontier'
            good = None
        else:
            if site_bal is not None and bal != site_bal:
                good = False
                note = f'balance differs by {bal - site_bal} sat'
            if site_tx is not None and txc != site_tx:
                good = False
                note = (note + '; ' if note else '') + f'tx_count {txc} vs site {site_tx}'
            if first_act is not None and fh != first_act:
                good = False
                note = (note + '; ' if note else '') + f'first_height {fh} vs site {first_act}'
            # the on demand path fills these in for addresses it can afford to sum
            if api.get('totalReceivedSat') is not None and recv != api['totalReceivedSat']:
                good = False
                note = (note + '; ' if note else '') + f"received {recv} vs site {api['totalReceivedSat']}"
            if api.get('totalSentSat') is not None and sent != api['totalSentSat']:
                good = False
                note = (note + '; ' if note else '') + f"sent {sent} vs site {api['totalSentSat']}"
        verdict = 'OK' if good is True else ('SKIP' if good is None else 'MISMATCH')
        if good is False:
            ok = False
        log(f'{a:<64} {bal/1e8:>18,.8f} {(site_bal or 0)/1e8:>18,.8f}  {txc:>7,}/{(site_tx or 0):>7,}  {verdict} {note}')
    log('')
    log('verification PASSED' if ok else 'verification FAILED')
    return ok


# ---- entry points -----------------------------------------------------------
def run_full(workers, fold_threads, scan_only, fold_only):
    tip = rpc('getblockcount')
    target = tip - CONFIRM_LAG
    target_hash = rpc('getblockhash', [target])
    recent = [{'height': h, 'hash': rpc('getblockhash', [h])}
              for h in range(max(0, target - 19), target + 1)]
    log(f'full build to height {target} (tip {tip})')
    conn = connect()
    cur = conn.cursor()
    ensure_schema(cur)
    conn.close()

    if not fold_only:
        run_scan(target, workers)
        st = load_state()
        if not st or st.get('height', -1) < target:
            log('scan did not reach the target; stopping before the fold')
            return
    if scan_only:
        log('scan only requested; stopping')
        return
    run_fold(fold_threads)
    build_index_and_publish(target, target_hash, recent)
    log('now catching the index up to the current tip')
    run_incremental()
    run_verify()
    # bucket files have done their job
    for b in range(BUCKETS):
        p = os.path.join(WORK_DIR, f'b{b:03d}.gz')
        if os.path.exists(p):
            os.unlink(p)
    if os.path.exists(STATE_PATH):
        os.unlink(STATE_PATH)
    log('bucket files removed')


def run_status():
    conn = connect()
    cur = conn.cursor()
    cur.execute(f"SELECT to_regclass('{LIVE_TABLE}'), to_regclass('{BUILD_TABLE}')")
    live, build = cur.fetchone()
    tip = rpc('getblockcount')
    print(f'chain tip: {tip}')
    if live:
        meta = read_meta(cur)
        print(f"indexed through: {meta['height'] if meta else 'unknown'} "
              f"({tip - meta['height'] if meta else '?'} behind tip)")
        cur.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{LIVE_TABLE}'))")
        print(f'address_stats size: {cur.fetchone()[0]}')
    st = load_state()
    if st:
        print(f"scan checkpoint: height {st['height']} of {st['target']} "
              f"({sum(st['lengths'])/1e9:.1f}GB of bucket files)")
    cur.execute(f"SELECT to_regclass('{FOLD_TABLE}')")
    if cur.fetchone()[0]:
        cur.execute(f'SELECT count(*), sum(rows_in), sum(rows_out) FROM {FOLD_TABLE}')
        n, ri, ro = cur.fetchone()
        print(f'buckets folded: {n}/{BUCKETS}, {ri or 0:,} rows in, {ro or 0:,} addresses out')
    if build:
        cur.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{BUILD_TABLE}'))")
        print(f'build table size: {cur.fetchone()[0]}')


def run_bench(n, workers):
    tip = rpc('getblockcount')
    start = tip - n
    t0 = time.time()
    pool = Pool(processes=workers, initializer=_worker_init)
    rows = 0
    nbytes = 0
    try:
        for h, parts in pool.imap_unordered(_worker_task, range(start, tip), chunksize=2):
            for d in parts.values():
                nbytes += len(d)
                rows += d.count(b'\n')
    finally:
        pool.terminate()
        pool.join()
    dt = time.time() - t0
    log(f'{n} blocks in {dt:.1f}s = {n/dt:.1f} blk/s with {workers} workers, '
        f'{rows:,} rows, {nbytes/1e6:.0f}MB raw')


def main():
    ap = argparse.ArgumentParser(description='build and maintain the openbitcoin address index')
    ap.add_argument('--full', action='store_true', help='full build from genesis (resumes)')
    ap.add_argument('--incremental', action='store_true', help='apply new blocks to a published index')
    ap.add_argument('--verify', action='store_true', help='compare sample addresses with the site')
    ap.add_argument('--status', action='store_true', help='report progress')
    ap.add_argument('--bench', type=int, default=0, help='time a scan of N recent blocks')
    ap.add_argument('--workers', type=int, default=8, help='parallel RPC/scan workers')
    ap.add_argument('--fold-threads', type=int, default=3, help='parallel bucket folders')
    ap.add_argument('--scan-only', action='store_true')
    ap.add_argument('--fold-only', action='store_true')
    ap.add_argument('--address', action='append', help='extra address to verify')
    args = ap.parse_args()

    if args.bench:
        return run_bench(args.bench, args.workers)
    if args.status:
        return run_status()

    os.makedirs(WORK_DIR, exist_ok=True)
    lock = open(LOCK_PATH, 'w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log('another address-index run holds the lock; exiting quietly')
        return

    if args.verify:
        return None if run_verify(args.address) else sys.exit(1)
    if args.full:
        return run_full(args.workers, args.fold_threads, args.scan_only, args.fold_only)
    return run_incremental()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'address-index FAILED: {type(e).__name__}: {e}', file=sys.stderr)
        sys.exit(1)
