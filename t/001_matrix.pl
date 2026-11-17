use strict;
use warnings;

use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql wait_until lsn_ge);
use Test::More;

my $primary = new_node('matrix_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$primary->start;

psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_payload (id bigint PRIMARY KEY, payload text)');

$primary->backup('matrix', backup_options => [ '-X', 'stream' ]);
my $standby = new_node('matrix_standby');
$standby->init_from_backup($primary, 'matrix', has_streaming => 1);
$standby->append_conf('postgresql.conf', "restore_command = ''");
$standby->start;

psql($standby, 'SELECT pg_wal_replay_pause()');
ok(wait_until($standby,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'", 't', 60),
	'replay is paused before additional WAL arrives');
my $replay_before = psql($standby, 'SELECT pg_last_wal_replay_lsn()');

psql($primary,
	"INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) "
	  . "FROM generate_series(1, 180000) AS g");
psql($primary, 'CHECKPOINT');
psql($primary, 'SELECT pg_switch_wal()');
psql($primary, 'SELECT pg_switch_wal()');
my $target = psql($primary, 'SELECT pg_current_wal_flush_lsn()');

ok(wait_until($standby,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$target'::pg_lsn, false)",
	't', 240), 'standby received WAL through the target');

$primary->stop('fast');
ok(wait_until($standby,
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver', 't', 30),
	'WAL receiver has stopped');

my $receive_fenced = psql($standby, 'SELECT pg_last_wal_receive_lsn()');
my $got = psql($standby, 'SELECT lwaldump()');
ok(lsn_ge($standby, $got, $receive_fenced),
	'lwaldump reaches at least the fenced receive LSN');
ok(lsn_ge($standby, $receive_fenced, $replay_before),
	'received WAL is no older than paused replay');

diag("replay before=$replay_before, fenced receive=$receive_fenced, lwaldump=$got");

$standby->stop('immediate');
done_testing();
