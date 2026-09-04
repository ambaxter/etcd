# PostgreSQL ETCD (PG ETCD)

## The What

PG ETCD is a proof of concept to replace Etcd's database (BoltDB) with PostgreSQL. Since ETCD is so critical to OpenShift, I have only carved out the database layer. With the effort I've invested *any other database* is now low hanging fruit.

As far as ETCD is concerned nothing has changed. The Raft algorithm controls the cluster’s resilience. PostgreSQL is merely an implementation detail for how the data is written and retrieved on disk. PostgreSQL would never be made available over the network, though it currently is for testing purposes. Customers should never *NEVER NEVER NEVER* be able to use their own database cluster for ETCD. ETCD is very sensitive to latency and anything but direct disk access will kill performance.

In particular, I’m using OrioleDb which is a new, more modern Postgres engine. It is designed to avoid the fragment issue plaguing both Bolt and stock Postgres.

It works and it’s faster, despite ETCD writing two WALs (Write-Ahead Logging): one because ETCD still thinks it’s using BoltDB and the other is PostgreSQL’s. It’s definitely not enterprise ready, yet, but my work shows it can be done. 

## The Output
For my leadership training, I will be publishing at least 3 blog posts regarding this topic. Topics will include database design, performance considerations and results, pitfalls, and directions for future work. 

The performance section will showcase a testing thunderdome:
* Backends
    * BoltDb
    * Postgresql 18.1
    * OrioleDb 17 (beta-13)
* Schema Layouts (pg_backend.go -> PgKvType Enum)
    * bucket - Byte identical to BoltDB using Postgresql tables
    * bucket_keys - Byte identical to BoltDB using Postgresql tables. Keys are in their own table
    * lbr_now_norm - Single log table and a normalized "now" table. "now" is the current list of keys and refers back to the log table for data.
    * lbr_now_nifs - Same as lbr_now_norm, but "now" is not normalized. 
    * lbr_kid_now_norm - Same as lbr_now_norm, but keys are not duplicated in the log
    * lbr_kid_now_nifs - Same as lbr_now_nifs, but keys are not duplicated in the log
* Compression
    * OrioleDb can compress (levels 1-22):
        * Table Data Structures
        * Primary Keys
        * Toast Values
    * Postgresql (and OrioleDb) can compress the WAL and TOAST values using:
        * pglz
        * lz4

Note: "nifs" stands for ["Normalization Is For Sissies"](https://www.infoq.com/news/2007/08/denormalization/). Most other blogs from that era are gone :/

## The Why

[Because BoltDB is a bad database.](https://news.ycombinator.com/item?id=30015913)

> BoltDB author here. Yes, it is a bad design. The project was never intended to go to production but rather it was a port of LMDB so I could understand the internals. I simplified the freelist handling since it was a toy project. At Shopify, we had some serious issues at the time (~2014) with either LMDB or the Go driver that we couldn't resolve after several months so we swapped out for Bolt. And alas, my poor design stuck around.

> LMDB uses a regular bucket for the freelist whereas Bolt simply saved the list as an array. It simplified the logic quite a bit and generally didn't cause a problem for most use cases. It only became an issue when someone wrote a ton of data and then deleted it and never used it again. Roblox reported having 4GB of free pages which translated into a giant array of 4-byte page numbers.

BoltDb is not an actual database. It is a copy-on-write kv b-tree index. It has no concepts of a WAL, data types other than byte arrays, or disk page free lists that aren’t a giant list of int64.

As such, Bolt is prone to many issues. For example, Etcd had to add their own WAL and create a custom locking scheme depending on which part in the program is working on the database. The initial issue that spurred me to exploration is a persistent problem at around the 8GB mark (The dreaded "8GB Wall") where the Bolt index becomes so fragmented performance tanks. 

Etcd is further hampered by:
* Bolt stores both keys and values as a pair. Right next to each other. On the index. Even medium sized values will blow up the number of btree nodes required to walk the index.
* Etcd's BoltDb btree keys are log revisions. Where an Etcd key resides on the index is a function of when it was stored, not which key it's next to lexicographically. Data locality? What's that?

Despite writing 2 WALs (1.5 according to Josh Berkus) and the overhead of copying data between processes, I'm seeing a 5-20% performance uplift depending on the exact configuration. Database size, especially with compression enabled, is orders of magnitude smaller. Admittedly, Etcd's benchmark tool slings heavily compressable values, but all's fair in love and benchmarks.

## Proof of Concept

### Current State

What I have created is a mostly functioning ETCD service that's been hardcore tested for `benchmark txn-mixed`. 

* It does not handle Postgresql connection resets
* It does not handle being restarted (Restore expects a BoltDB file)
* It does not handle being clustered with the current compose files
* It exposes the Postgresql ports for debugging purposes, but connects via the socket if available

I know more about software development and databases than I do about the finer points of ETCD. 

### Dependencies 
* Podman Compose (though Docker should also work)
* [OrioleDb 17](https://github.com/orioledb/orioledb) - [beta13](https://github.com/orioledb/orioledb/releases/tag/beta13) - Modern Postgresql Engine
* [Postgresql](https://www.postgresql.org/) [18.1](https://www.postgresql.org/about/news/postgresql-181-177-1611-1515-1420-and-1323-released-3171/) - Postgresql
* [jackc/pgx](https://github.com/jackc/pgx) - High performance Postgresql Go Client
* [stephenafamo/bob](https://github.com/stephenafamo/bob) - Sql Query Library
* [tidwall/btree](https://github.com/tidwall/btree) - Btree Map Library that's actually maintained 

You're only required to install Podman Compose. Everything else will be downloaded during build or on `podman compose up`

### Source Code

Important files and directories:
* contrib/pgetcd/build - Docker buildfile for the rw-heatmap tests
* contrib/pgetcd/compose - Compose files for the etcd service in various configurations
* server/storage/backend/pg_*.go - the Postgres implementation files
    * pg_backend.go - Posgresql backend
    * pg_buffer.go - Btree kv caches 
    * pg_setup.go - Database Setup
    * pg_schema_*.go - Various schemas and associated SQL queries
    * pg_tx.go, pg_batch_tx.go - RO, RW interface

### Database Layouts

I hate runtime failues. I've designed the database setup to error out either at build or boot time as much as possible. 

* Queries get registered as PostgreSQL functions during Etcd setup to catch query errors and improve performance
* Queries get registered as Postgresql functions duing Etcd setup to catch query errors and improve performance
    * Annoyingly, OnConflict statements are only evaluated at runtime
* Table, index, and query definitions are bundled together  (pg_setup.go -> pgSchemaBundle).

The schemas are:
* bucket - pg_schema_bucket.go (always registered)
* bucket_keys - pg_schema_bucket_keys.go
* lbr_now_norm - pg_schema_log_by_rev.go -> kvLbrNowNormSchemaBundle
* lbr_now_nifs - pg_schema_log_by_rev.go -> kvLbrNowNifsSchemaBundle
* lbr_kid_now_norm - pg_schema_log_by_rev.go -> kvLbrKidNowNormSchemaBundle
* lbr_kid_now_nifs - pg_schema_log_by_rev.go -> kvLbrKidNowNifsSchemaBundle

### Container Image Build

#### Linux

`make build && make tools && podman build -t pgetcd -f contrib/pgetcd/build/Dockerfile bin`

#### ARM Mac with Podman Desktop

`make build-linux-arm64 && make tools-linux-arm64 && podman build -t pgetcd -f contrib/pgetcd/build/Dockerfile bin`

### Running Unit Tests

I have not polished the unit tests, but most should work fine.

Requires:
* Postgresql Deployed
    * OrioleDb: `podman compose -f contrib/pgetcd/compose/db/oriole17_compose.yaml up`
    * Postgresql: `podman compose -f contrib/pgetcd/compose/db/postgres18_compose.yaml up`
* server/storage/backend/backend.go -> TEST_POSTGRES = true

### Running The Etcd Service

Note
* Volumes must be destroyed between sessions. Etcd expects the Bolt snapshot to exist on a restart.
* Clustering is not available at this time

#### OrioleDb
* up: `podman compose -f contrib/pgetcd/compose/db/oriole17_compose.yaml --profile etcd up`
* down: `podman compose -f contrib/pgetcd/compose/db/oriole17_compose.yaml --profile etcd down`

ENV Variables:
* KV_TYPE: database schema. Defaults to bucket
    * bucket
    * bucket_keys
    * lbr_now_norm
    * lbr_now_nifs
    * lbr_kid_now_norm
    * lbr_kid_now_nifs
* CMP_DEF: OrioleDb Table compression. Defaults to disabled
    * -1: Disabled
    * 1 - 22: Compression level
* CMP_PRI: OrioleDb Primary Key compression. Defaults to disabled
    * -1: Disabled
    * 1 - 22: Compression level
* CMP_TST: OrioleDb TOAST compression. Defaults to disabled
    * -1: Disabled
    * 1 - 22: Compression level
* CMP_WAL: Write Ahead Log Compression. Defaults to off
    * off
    * pglz
    * lz4

#### Postgresql
* up: `podman compose -f contrib/pgetcd/compose/db/postgres18_compose.yaml --profile etcd up`
* down: `podman compose -f contrib/pgetcd/compose/db/postgres18_compose.yaml --profile etcd down`

ENV Variables:
* CMP_WAL: Write Ahead Log Compression. Defaults to off
    * off
    * pglz
    * lz4


### Running Benchmarks

Benchmarks can be run using `tools/pg-thunderdome/pg-thunderdome.sh`. 

Requires:
* pgetcd container image

Options:
* Backend type:
    * -o : OrioleDB
    * -d : PostgreSQL
    * neither: BoltDB

ENV Variables:
* PG_KV_TYPE_LIST: Database schema to benchmark in order. 
    * bucket
    * bucket_keys
    * lbr_now_norm
    * lbr_now_nifs
    * lbr_kid_now_norm
    * lbr_kid_now_nifs
* PG_CMP_ORIOLE_LIST: List of OrioleDB compression option: "-1 1 9"
    * -1: Disabled
    * 1 - 22: Compression level
* PG_CMP_WAL_LIST: List of Write Ahead Log Compression options: "off pglz lz4"
    * off
    * pglz
    * lz4

## Todos Before Blog Posts:
- [x] Separate individual schemas in to their own files
- [x] Configurable wal_compression (compose files)
- [x] Less brittle pg storage/backend unit test capability
- [x] Better test schema naming convention (visible test name, kvtype, timestamp for better understanding when debugging)
- [x] Remove test schemas that pass
- [x] Fix up PG_DEBUG outputs. Make it configurable
- [x] Add compact to benchmark output
- [x] Add defrag to benchmark output
- [x] Finalized test schemas!
- [x] Run performance thunderdome
- [ ] Analyze performance thunderdome
- [x] God that's like another month of work...

## Todos Before Actually Usable:
- [ ] Hash - I have not looked into what guarantees are required *or* how they're validated
- [ ] Snapshot - depends heavily on "let's copy a file"
- [ ] Restore - Expects BoltDb file at start
- [ ] Clusterable - Requires all of the above
- [ ] Unit tests require no manual postgres deployment
- [ ] Passes all unit tests - R
- [ ] Removing Etcd's WAL

## Todos Before It's Enterprise Ready:
- [ ] Determine table layout
- [ ] Handle connection reset (posgres fault)
- [ ] Panics... panics everywhere
- [ ] Harden Postgresql access, security
- [ ] Better setup functionality
- [ ] Migration functionality (always a difficult prospect)

## Todos I'd Really like
- [ ] Compact hash function inside Postgresql
- [ ] LockInsideApply vs. LockOutsideApply - just why
- [ ] lbr_kid_now_norm, lbr_kid_now_nifs - split insert key statements out