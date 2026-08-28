# lwaldump
Tool for finding the end of the valid WAL prefix stored in a replica's local
`pg_wal`. It is required for safe quorum failover.

`pg_last_wal_receive_lsn()` is kept in walreceiver state and is lost when
PostgreSQL restarts. `pg_last_wal_replay_lsn()` may still be behind WAL that
walreceiver had already flushed before that restart, even after hot standby is
available for queries. Electing a primary by either value can therefore ignore
an acknowledged commit that remains on disk and let an older replica win.
`lwaldump()` reconstructs the durable endpoint by reading the WAL files
themselves. Callers must fail closed if the scan fails; receive/replay LSN is
not a safe fallback.

Usage on primary:
CREATE EXTENSION lwaldump;

Usage on standby:
SELECT lwaldump(); 
