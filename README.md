# address-index

Two builders that turn your own Bitcoin Core node into the address statistics
behind [openbitcoin.com](https://openbitcoin.com):

- `build-address-index.py` builds and maintains `address_stats`: lifetime
  received, lifetime sent, transaction count and activity span for every
  address that has ever appeared in a standard output on mainnet.
- `build-rich-list.py` builds `rich_list`, `rich_distribution` and
  `chain_stats` from a UTXO set snapshot: the top balances, the balance
  distribution, the number of funded addresses, and the measured circulating
  supply.

Everything is computed from bitcoind and published to PostgreSQL. No
third-party data source is involved at any point.

## How openbitcoin.com uses it

- `address_stats` backs the explorer's address pages: lifetime totals,
  transaction count and first/last activity for any address, served from the
  index instead of being summed on demand.
- `rich_list` and `rich_distribution` back the
  [/rich-list](https://openbitcoin.com/rich-list) page: the top 10,000
  balances and the distribution table.
- `chain_stats` backs [/stats](https://openbitcoin.com/stats) and the site
  footer figures: funded addresses, the exact supply measured at the snapshot
  height, and the raw blockchain size.

## Why both scripts share a repository

They are two halves of the same address-statistics layer. The address index
gives lifetime totals per address from a full chain scan; the rich list gives
current balances from a UTXO snapshot. The site joins their outputs (the
rich-list page reads `rich_list` joined against `address_stats`), and the
scripts share the same node, the same database, the same env-file
configuration, the same lock-file discipline and the same `job_heartbeats`
monitoring table. Split apart, each repo would only make sense next to the
other.

## Architecture

### Address index

One forward pass over the chain sees both sides of every transaction:
`getblock <hash> 3` returns every input with its `prevout` inline, address and
value included, so no separate UTXO map is needed. Worker processes each fold
one block into per-address deltas and hand back compact text lines; the parent
routes each line into one of 256 append-only gzip bucket files, chosen by
crc32 of the address, so every row for a given address lands in exactly one
bucket. The fold phase then aggregates each bucket on its own with a plain SQL
`GROUP BY` via `COPY`, appends the result to a build table with no index, adds
the primary key once at the end from sorted input, and renames the table into
place inside one transaction. Roughly 5.7 billion address touches never live
in Python at once.

The scan checkpoints the completed height together with the exact byte length
of all 256 bucket files, so an interrupted build truncates back to the
checkpoint and resumes without double counting. It is also polite to a shared
box: workers run niced, and the scan watches the node's own `getblockcount`
latency, pausing when it climbs so that a co-hosted electrum server (Fulcrum,
on the production box) keeps serving.

`--incremental` extends a published index block by block, at most 240 blocks
per run. It checks the indexed tip's hash against the chain on every run
and, when that check fails, walks the last 20 recorded hashes to find the
fork point; the stale blocks are re-fetched (Core keeps them on disk) and
undone with negated deltas before it advances.
Unconfirmed transactions never enter the index. `--verify` compares sample
addresses and the top rich-list rows against the site's own address route;
that mode assumes openbitcoin.com's data service on `127.0.0.1:8090` and is
only meaningful next to it.

### Rich list

`dumptxoutset` writes a serialized snapshot of the whole UTXO set. The parser
streams that file (version 2 format: header, then coins grouped by txid, with
CompactSize and Bitcoin VARINT fields exactly as Core writes them) and pipes
one line per address-bearing output straight into an UNLOGGED staging table
with `COPY`; peak Python memory is a few megabytes. Before anything is
published, an integrity gate requires the parsed coin count and satoshi total
to match the node's own `coinstatsindex` figures for the snapshot height
exactly; a desynced parse can never reach the public tables. SQL then does
the per-address `GROUP BY`, the top-N sort and a `width_bucket` distribution
over every funded balance. The ranked rows, the distribution and the chain
stats swap in a single transaction, so a reader sees either the previous list
or the new one, never a mix.

## Requirements

- Linux (both scripts use `fcntl` file locks), Python 3.8 or newer, `psycopg2`
- PostgreSQL with real disk headroom. The full address-index build stages tens
  of GB of bucket files and grows a build table of over a billion rows toward
  roughly a hundred GB; the scan stops if free space falls below 60 GB. The
  rich list needs about 40 GB free for the snapshot (~11 GB) plus staging
  (~16 GB).
- Bitcoin Core:
  - address index: `getblock` verbosity 3 with inline `prevout` data (the
    script was written and verified against v31). No `txindex` needed;
    verbosity 3 supplies the prevouts itself.
  - rich list: `dumptxoutset` producing the version 2 snapshot format (v29 or
    newer) and `coinstatsindex=1`, which the integrity gate reads via
    `gettxoutsetinfo none <height>`.
- The rich list runs `bitcoin-cli` as the `bitcoin` user (via
  `sudo -n -u bitcoin` unless already running as that user), because `dumptxoutset` and
  `gettxoutsetinfo` are deliberately absent from the web-facing RPC whitelist
  on the production box. Arrange the same, or run it as a user that can call
  those RPCs.

## Configuration

Both scripts read two plain `KEY=value` files:

- `/etc/openbitcoin/rpc-obweb.env`: `RPC_HOST` (default `127.0.0.1`),
  `RPC_PORT` (default `8332`), `OBWEB_RPC_USER`, `OBWEB_RPC_PASSWORD`
- `/etc/openbitcoin/db.env`: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`,
  `PGPASSWORD`

Paths are constants near the top of each script, published exactly as they run
in production; adjust them to your box: the work directory
`/var/lib/openbitcoin/addridx`, the snapshot directory
`/var/lib/openbitcoin/utxo`, lock files under `/var/lock`,
`/usr/local/bin/bitcoin-cli`, and `/home/bitcoin/.bitcoin/blocks` (read only
to measure the raw `blk*.dat` size for `chain_stats`).

Both scripts create their own output tables, but they report into a shared
monitoring table you create once:

```sql
CREATE TABLE IF NOT EXISTS job_heartbeats (
  job text PRIMARY KEY,
  last_success timestamptz,
  detail text
);
```

## Running

Address index:

```
./build-address-index.py --bench 200        # time a short scan on your hardware first
./build-address-index.py --full             # full build from genesis; resumes if interrupted
./build-address-index.py --status           # where the build has got to
./build-address-index.py --incremental      # apply new blocks; the forever mode
./build-address-index.py --verify           # only meaningful next to the site's data service
```

The full build scans every block over RPC and its speed is set by your node
and disks; the log prints blocks per second and an ETA, and on any hardware it
is a job measured in hours to days. Interrupting is safe: it checkpoints every
2,000 blocks and `--full` resumes from the checkpoint. Once published,
schedule `--incremental` frequently; production triggers it on every new
block, and a cron entry every few minutes works too. A lock file makes an
overlapping run exit quietly, and one run applies at most 240 blocks so a
catch-up cannot balloon.

Rich list:

```
./build-rich-list.py --dry-run    # dump, parse and rank, but publish nothing
./build-rich-list.py              # dump, parse, verify, publish, delete the snapshot
```

Production runs it from cron as root. `--snapshot PATH` reuses an existing
dump instead of writing a new one, and `--keep` leaves the snapshot file
behind.

## Data honesty

Address index:

- An address is only what Bitcoin Core itself names in
  `scriptPubKey.address`. Bare pubkey (P2PK), bare multisig, OP_RETURN and
  non-standard scripts have no address and are left out rather than invented.
  That deliberately matches what an electrum-style index reports, so these
  totals agree with the site's live address route. One visible consequence:
  the genesis coinbase is a P2PK output, so the totals for
  `1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa` begin at height 123723, its first
  appearance in a standard output.
- Confirmed blocks only; mempool transactions never enter the index.
- A meta row records the height the index is complete through; a row in
  `address_stats` is only authoritative for activity at or below that height.
- After a reorg, balances and counts are exact (the undo applies negated
  deltas), but `first_height` and `last_height` for the affected addresses are
  approximate until those addresses next move.

Rich list:

- It ranks addresses, not owners. One entity can hold thousands of addresses,
  and one address (an exchange cold wallet) can hold funds for millions of
  people. No clustering is attempted or implied.
- Outputs with no address are excluded from the ranking and counted: every run
  logs how many outputs, and how much BTC, were held out.
- `first_seen` is the block date of the oldest coin the address still holds,
  which is not the same as the address first appearing on chain.
- The list is a snapshot at one height, refreshed on the cron cadence, not a
  live view.
- The integrity gate guarantees the parse matches the node exactly, coin count
  and total satoshis both; it does not make an address-based ranking mean more
  than it does.

## License

MIT. The OpenBitcoin name and logo are not part of the license.
