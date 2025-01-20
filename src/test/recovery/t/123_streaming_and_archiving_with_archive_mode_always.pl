use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 8;
use File::Copy;

# Initialize master node with WAL archiving setup
my $node_master = get_new_node('master');
$node_master->init(has_archiving => 1, allows_streaming => 1);

$node_master->append_conf('postgresql.conf', "archive_command = 'exit 1'");
$node_master->append_conf('postgresql.conf', 'wal_sender_archiving_status_interval = 50ms');
$node_master->start;
my $master_archive = $node_master->archive_dir;
my $master_data = $node_master->data_dir;

# Take backup
$node_master->backup('my_backup');

# Initialize the standby node
my $node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, 'my_backup', has_streaming => 1);
$node_standby->append_conf('postgresql.conf', 'archive_mode = always');
my $standby_archive = $node_standby->archive_dir;
my $standby_data = $node_standby->data_dir;
$node_standby->start;

$node_master->safe_psql('postgres', "CREATE TABLE test1 AS SELECT generate_series(1,10) AS x;");

my $walfile = $node_master->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_current_xlog_location());");
$node_master->safe_psql('postgres', "SELECT pg_switch_xlog(); CHECKPOINT;");

my $walfile2 = $node_master->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_current_xlog_location());");

# After switching wal, the current wal file will be marked as ready to be archived on the master. But this wal file
# won't get archived because of the incorrect archive_command
my $walfile_ready = "pg_xlog/archive_status/$walfile.ready";
my $walfile_done = "pg_xlog/archive_status/$walfile.done";

# Wait for the standby to catch up
$node_master->wait_for_catchup($node_standby, 'write',
	$node_master->lsn('insert'));

# Wait for archive failure
$node_master->poll_query_until('postgres', "SELECT failed_count > 0 FROM pg_stat_archiver", 't') or die "Timed out while waiting for archiving to fail";

ok( -f "$master_data/$walfile_ready", ".ready file exists on master for WAL segment $walfile");
ok( -f "$standby_data/$walfile_ready", ".ready file exists on standby for WAL segment $walfile");

ok( !-f "$master_data/$walfile_done", ".done file does not exist on master for WAL segment $walfile");
ok( !-f "$standby_data/$walfile_done", ".done file does not exist on standby for WAL segment $walfile");

# Make WAL archiving work again for master by resetting the archive_command
$node_master->safe_psql('postgres', "ALTER SYSTEM SET archive_command = 'echo %p && cp %p $master_archive/%f'; SELECT pg_reload_conf();");

# Force the archiver process to wake up and start archiving
$node_master->safe_psql('postgres', "SELECT pg_switch_xlog();");

# Check if master has .done file created for the archived segment and also that the file gets uploaded to the archive
wait_until_file_exists("$master_data/$walfile_done", ".done file to exist on master for WAL segment $walfile");

ok( !-f "$master_data/$walfile_ready", ".ready file does not exist for WAL segment $walfile");

wait_until_file_exists("$master_archive/$walfile", "$walfile to be archived by the master");

run_cmd_until(["grep", "sending archival report: $walfile2", $node_master->logfile], "sending archival report: $walfile2") or die "Timed out while waiting for sending archival report: $walfile2";

ok( -f "$standby_data/$walfile_ready", ".ready file exists on standby for WAL segment $walfile");
ok( !-f "$standby_data/$walfile_done", ".done file does not exist on standby for WAL segment $walfile");
ok( !-f "$standby_archive/$walfile", "$walfile does not exist in standby's archive");
