# lwaldump

`lwaldump` finds the end of the valid WAL prefix stored in a replica's local
`pg_wal`. It is intended for safe quorum failover.

## Why SQL replication positions are insufficient

`pg_last_wal_receive_lsn()` is kept in walreceiver state and is lost when
PostgreSQL restarts. `pg_last_wal_replay_lsn()` may still be behind WAL that
walreceiver had already flushed before that restart, even after hot standby is
available for queries. Electing a primary by either value can therefore ignore
an acknowledged commit that remains on disk and let an older replica win.
`lwaldump()` reconstructs the durable endpoint by reading the WAL files
themselves. Callers must fail closed if the scan fails; receive/replay LSN is
not a safe fallback.

## PostgreSQL 14-19 compatibility

The extension is backend code. It must not define `FRONTEND` or include
frontend-only headers. Starting with PostgreSQL 15,
`GetXLogReplayRecPtr()` is declared in `access/xlogrecovery.h`; the extension
includes that header conditionally. Compiling without the declaration can
either fail or, depending on compiler settings, incorrectly treat the 64-bit
`XLogRecPtr` result as an `int`.

PostgreSQL 15 also introduced `NextRecPtr` in `XLogReaderState`. Assigning only
`EndRecPtr` leaves the next read position at zero and makes the reader try to
open WAL segment `000000010000000000000000`. `XLogBeginRead()` is the supported
way to initialize the reader and works on PostgreSQL 14 through 19.

`XLogFindNextRecord()` is not needed here: the replay LSN is already the start
position for the scan, and that function's signature changes in PostgreSQL 19.

## Usage

Build and install against the selected PostgreSQL:

```sh
make PG_CONFIG=/path/to/pg_config
make PG_CONFIG=/path/to/pg_config install
```

Create the extension:

```sql
CREATE EXTENSION lwaldump;
```

Read the local durable WAL endpoint on a standby:

```sql
SELECT lwaldump();
```

The function requires a replay position and readable local WAL files. Any
scan error is an error, not a reason to fall back to SQL receive/replay LSNs.
