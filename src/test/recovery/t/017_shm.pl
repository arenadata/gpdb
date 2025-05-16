#
# Tests of pg_shmem.h functions
#
use strict;
use warnings;
use Config;
use File::stat qw(stat);
use IPC::Run 'run';
use PostgresNode;
use Test::More;
use TestLib;
use Time::HiRes qw(usleep);

# If we don't have shmem support, skip the whole thing
eval {
	require IPC::SharedMem;
	IPC::SharedMem->import;
	require IPC::SysV;
	IPC::SysV->import(qw(IPC_CREAT IPC_EXCL S_IRUSR S_IWUSR));
};
if ($@ || $windows_os)
{
	plan skip_all => 'SysV shared memory not supported by this platform';
}
else
{
	plan tests => 4;
}

my $tempdir = TestLib::tempdir;

# Log "ipcs" diffs on a best-effort basis, swallowing any error.
my $ipcs_before = "$tempdir/ipcs_before";
eval { run_log [ 'ipcs', '-am' ], '>', $ipcs_before; };

sub log_ipcs
{
	eval { run_log [ 'ipcs', '-am' ], '|', [ 'diff', $ipcs_before, '-' ] };
	return;
}

<<<<<<< HEAD
# These tests need a $port such that nothing creates or removes a segment in
# $port's IpcMemoryKey range while this test script runs.  While there's no
# way to ensure that in general, we do ensure that if PostgreSQL tests are the
# only actors.  With TCP, the first get_new_node picks a port number.  With
# Unix sockets, use a postmaster, $port_holder, to represent a key space
# reservation.  $port_holder holds a reservation on the key space of port
# 1+$port_holder->port if it created the first IpcMemoryKey of its own port's
# key space.  If multiple copies of this test script run concurrently, they
# will pick different ports.  $port_holder postmasters use odd-numbered ports,
# and tests use even-numbered ports.  In the absence of collisions from other
# shmget() activity, gnat starts with key 0x7d001 (512001), and flea starts
# with key 0x7d002 (512002).
my $port_holder;
if (!$PostgresNode::use_tcp)
{
	my $lock_port;
	for ($lock_port = 511; $lock_port < 711; $lock_port += 2)
	{
		$port_holder = PostgresNode->get_new_node(
			"port${lock_port}_holder",
			port     => $lock_port,
			own_host => 1);
		$port_holder->init;
		# GPDB: minimum max_connections is 10 in GPDB
		$port_holder->append_conf('postgresql.conf', 'max_connections = 10');
		$port_holder->start;
		# Match the AddToDataDirLockFile() call in sysv_shmem.c.  Assume all
		# systems not using sysv_shmem.c do use TCP.
		my $shmem_key_line_prefix = sprintf("%9lu ", 1 + $lock_port * 1000);
		last
		  if slurp_file($port_holder->data_dir . '/postmaster.pid') =~
		  /^$shmem_key_line_prefix/m;
		$port_holder->stop;
	}
	$port = $lock_port + 1;
}

# Node setup.
sub init_start
{
	my $name = shift;
	my $ret = PostgresNode->get_new_node($name, port => $port, own_host => 1);
	defined($port) or $port = $ret->port;    # same port for all nodes
	$ret->init;
	# Limit semaphore consumption, since we run several nodes concurrently.
	# GPDB: minimum max_connections is 10 in GPDB
	$ret->append_conf('postgresql.conf', 'max_connections = 10');
	$ret->start;
	log_ipcs();
	return $ret;
}
my $gnat = init_start 'gnat';
my $flea = init_start 'flea';
=======
# Node setup.
my $gnat = PostgresNode->get_new_node('gnat');
$gnat->init;

# Create a shmem segment that will conflict with gnat's first choice
# of shmem key.  (If we fail to create it because something else is
# already using that key, that's perfectly fine, though the test will
# exercise a different scenario than it usually does.)
my $gnat_dir_stat = stat($gnat->data_dir);
defined($gnat_dir_stat) or die('unable to stat ' . $gnat->data_dir);
my $gnat_inode = $gnat_dir_stat->ino;
note "gnat's datadir inode = $gnat_inode";

# Note: must reference IPC::SysV's constants as functions, or this file
# fails to compile when that module is not available.
my $gnat_conflict_shm =
  IPC::SharedMem->new($gnat_inode, 1024,
	IPC_CREAT() | IPC_EXCL() | S_IRUSR() | S_IWUSR());
note "could not create conflicting shmem" if !defined($gnat_conflict_shm);
log_ipcs();

$gnat->start;
log_ipcs();

$gnat->restart;    # should keep same shmem key
log_ipcs();
>>>>>>> eb57bd9c1d83a20eaff559a53b2f584dcd0668a8

# Upon postmaster death, postmaster children exit automatically.
$gnat->kill9;
log_ipcs();
poll_start($gnat);    # gnat recycles its former shm key.
log_ipcs();

note "removing the conflicting shmem ...";
$gnat_conflict_shm->remove if $gnat_conflict_shm;
log_ipcs();

# Upon postmaster death, postmaster children exit automatically.
$gnat->kill9;
log_ipcs();

# In this start, gnat will use its normal shmem key, and fail to remove
# the higher-keyed segment that the previous postmaster was using.
# That's not great, but key collisions should be rare enough to not
# make this a big problem.
poll_start($gnat);
log_ipcs();
$gnat->stop;
log_ipcs();

# Re-create the conflicting segment, and start/stop normally, just so
# this test script doesn't leak the higher-keyed segment.
note "re-creating conflicting shmem ...";
$gnat_conflict_shm =
  IPC::SharedMem->new($gnat_inode, 1024,
	IPC_CREAT() | IPC_EXCL() | S_IRUSR() | S_IWUSR());
note "could not create conflicting shmem" if !defined($gnat_conflict_shm);
log_ipcs();

$gnat->start;
log_ipcs();
$gnat->stop;
log_ipcs();

note "removing the conflicting shmem ...";
$gnat_conflict_shm->remove if $gnat_conflict_shm;
log_ipcs();

# Scenarios involving no postmaster.pid, dead postmaster, and a live backend.
# Use a regress.c function to emulate the responsiveness of a backend working
# through a CPU-intensive task.
$gnat->start;
log_ipcs();

my $regress_shlib = TestLib::perl2host($ENV{REGRESS_SHLIB});
$gnat->safe_psql('postgres', <<EOSQL);
CREATE FUNCTION wait_pid(int)
   RETURNS void
   AS '$regress_shlib'
   LANGUAGE C STRICT;
EOSQL
my $slow_query = 'SELECT wait_pid(pg_backend_pid())';
my ($stdout, $stderr);
my $slow_client = IPC::Run::start(
	[
		'psql', '-X', '-qAt', '-d', $gnat->connstr('postgres'),
		'-c', $slow_query
	],
	'<',
	\undef,
	'>',
	\$stdout,
	'2>',
	\$stderr,
	IPC::Run::timeout(900));    # five times the poll_query_until timeout
ok( $gnat->poll_query_until(
		'postgres',
		"SELECT 1 FROM pg_stat_activity WHERE query = '$slow_query'", '1'),
	'slow query started');
my $slow_pid = $gnat->safe_psql('postgres',
	"SELECT pid FROM pg_stat_activity WHERE query = '$slow_query'");
$gnat->kill9;
unlink($gnat->data_dir . '/postmaster.pid');
$gnat->rotate_logfile;    # on Windows, can't open old log for writing
log_ipcs();
# Reject ordinary startup.  Retry for the same reasons poll_start() does.
my $pre_existing_msg = qr/pre-existing shared memory block/;
{
	my $max_attempts = 180 * 10;    # Retry every 0.1s for at least 180s.
	my $attempts     = 0;
	while ($attempts < $max_attempts)
	{
		last
		  if $gnat->start(fail_ok => 1)
		  || slurp_file($gnat->logfile) =~ $pre_existing_msg;
		usleep(100_000);
		$attempts++;
	}
}
like(slurp_file($gnat->logfile),
	$pre_existing_msg, 'detected live backend via shared memory');
# Reject single-user startup.
my $single_stderr;
ok( !run_log(
		[ 'postgres', '--single', '-D', $gnat->data_dir, 'template1' ],
		'<', \undef, '2>', \$single_stderr),
	'live query blocks --single');
print STDERR $single_stderr;
like($single_stderr, $pre_existing_msg,
	'single-user mode detected live backend via shared memory');
log_ipcs();

# cleanup slow backend
TestLib::system_log('pg_ctl', 'kill', 'QUIT', $slow_pid);
$slow_client->finish;    # client has detected backend termination
log_ipcs();

# now startup should work
poll_start($gnat);
log_ipcs();

# finish testing
$gnat->stop;
log_ipcs();


# We may need retries to start a new postmaster.  Causes:
# - kernel is slow to deliver SIGKILL
# - postmaster parent is slow to waitpid()
# - postmaster child is slow to exit in response to SIGQUIT
# - postmaster child is slow to exit after postmaster death
sub poll_start
{
	my ($node) = @_;

	my $max_attempts = 180 * 10;
	my $attempts     = 0;

	while ($attempts < $max_attempts)
	{
		$node->start(fail_ok => 1) && return 1;

		# Wait 0.1 second before retrying.
		usleep(100_000);

		$attempts++;
	}

	# No success within 180 seconds.  Try one last time without fail_ok, which
	# will BAIL_OUT unless it succeeds.
	$node->start && return 1;
	return 0;
}
