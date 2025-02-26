use strict;
use warnings;
use File::Basename qw(basename dirname);
use File::Compare;
use File::Path qw(rmtree);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Set umask so test directories and files are created with default permissions
umask(0077);

my $tempdir = PostgreSQL::Test::Utils::tempdir;
my $basebackupdir1 = $tempdir . '/basebackup1';
my $basebackupdir2 = $tempdir . '/basebackup2';
my $archivedir =  $tempdir . '/archive';

my $primary = PostgreSQL::Test::Cluster->new('primary');

sub create_backup_primary
{
	my ($backup_dir, $restore_point) = @_;

	print("Running: create_backup_primary: '$restore_point'\n");

	$primary->command_ok([ 'rm', '-rf', $backup_dir ],
		"clear directory for backup '$backup_dir'");
	mkdir($backup_dir);

	$primary->command_ok([ 'pg_basebackup', '--target-gp-dbid', 2, '-D', "$backup_dir", '-p', $primary->port, '--verbose' ],
		'pg_basebackup runs');
	ok(-f "$backup_dir/PG_VERSION", "backup was created at '$backup_dir'");

	$primary->safe_psql('test_db', "SELECT pg_create_restore_point('$restore_point')");
}

sub restore_backup_primary
{
	my ($backup_dir, $restore_point, $label) = @_;
	my $primary_pgdata = $primary->data_dir;

	print("Running: restore_backup_primary: '$restore_point' '$label'\n");

	$primary->command_ok([ 'rm', '-rf', $primary_pgdata ],
		"'$label': rm primary pgdata");
	$primary->command_ok([ 'cp', '-r', $backup_dir, $primary_pgdata ],
		"'$label': copy basebackup to primary");

	# configure recovery
	$primary->set_recovery_mode;
	open my $conf, '>', "$primary_pgdata/postgresql.auto.conf";
	print $conf "recovery_target_action = 'promote'\n";
	print $conf "restore_command = 'cp -vr $archivedir/%f $primary_pgdata/%p'\n";
	print $conf "recovery_target_name = '$restore_point'\n";
	close $conf;
	$primary->start;
}

sub wait_recovery_and_switch_wal_primary
{
	my ($label) = @_;

	print("Running: wait_recovery_and_switch_wal_primary: '$label'\n");

	$primary->poll_query_until('test_db', "SELECT true FROM pg_switch_wal()")
		or die "'$label': Timed out while waiting for switch WAL after recovery";
}

mkdir($archivedir);

# Initialize primary
$primary->init(allows_streaming => 1, extra => ['--data-checksums']);
$primary->append_conf("postgresql.conf", "archive_mode = 'on'");
$primary->append_conf("postgresql.conf", "archive_command = 'cp -rv %p $archivedir/'");
$primary->start;
$primary->safe_psql('postgres', 'CREATE DATABASE test_db');
$primary->safe_psql('test_db', 'CREATE TABLE test as select generate_series(1,10)');
$primary->safe_psql('postgres', 'CREATE ROLE postgres WITH LOGIN REPLICATION');


create_backup_primary($basebackupdir1, 'backup_label1');
$primary->stop('smart');
restore_backup_primary($basebackupdir1, 'backup_label1', '001 -> 012');
wait_recovery_and_switch_wal_primary('001 -> 012');

create_backup_primary($basebackupdir2, 'backup_label2');
$primary->stop('smart');
restore_backup_primary($basebackupdir2, 'backup_label2', '012 -> 013');
wait_recovery_and_switch_wal_primary('012 -> 013');

$primary->stop('smart');
restore_backup_primary($basebackupdir2, 'backup_label2', '012 -> 023');
wait_recovery_and_switch_wal_primary('012 -> 023');

$primary->stop('smart');
restore_backup_primary($basebackupdir1, 'backup_label1', '101 -> 112');
wait_recovery_and_switch_wal_primary('101 -> 112');

$primary->stop('smart');
restore_backup_primary($basebackupdir2, 'backup_label2', '112 -> 113');
wait_recovery_and_switch_wal_primary('112 -> 113');

$primary->stop('smart');
restore_backup_primary($basebackupdir2, 'backup_label2', '112 -> 123');
wait_recovery_and_switch_wal_primary('112 -> 123');

$primary->stop('smart');
restore_backup_primary($basebackupdir1, 'backup_label1', '201 -> 212');
wait_recovery_and_switch_wal_primary('201 -> 212');

$primary->stop('smart');
restore_backup_primary($basebackupdir2, 'backup_label2', '212 -> 213');
wait_recovery_and_switch_wal_primary('212 -> 213');

$primary->stop('smart');
restore_backup_primary($basebackupdir2, 'backup_label2', '212 -> 223');
wait_recovery_and_switch_wal_primary('212 -> 223');

$primary->teardown_node;

done_testing();
