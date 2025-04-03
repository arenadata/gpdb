-- test that interruption of DROP DATABASE is handled properly

!\retcode gpconfig -c autovacuum -v off;
!\retcode gpstop -au;

-- start_ignore
DROP DATABASE IF EXISTS regression_invalid_interrupt;
-- end_ignore
-- create the database
CREATE DATABASE regression_invalid_interrupt;

-- prevent drop database via lock on pg_tablespace on segment 0
0U: BEGIN;
0U: LOCK pg_tablespace;

-- try to drop, this will wait due to the still held lock on segment 0
1&: DROP DATABASE regression_invalid_interrupt;

-- ensure the DROP DATABASE is waiting for the lock
SELECT EXISTS (SELECT FROM pg_locks WHERE NOT granted AND
    relation = 'pg_tablespace'::regclass AND mode = 'AccessShareLock');

-- and finally interrupt the DROP DATABASE on segment 0
0U: SELECT pg_cancel_backend(pid) FROM pg_locks WHERE NOT granted AND
    relation = 'pg_tablespace'::regclass AND mode = 'AccessShareLock';
0Uq:

1<:
1q:

-- verify that connection to the database aren't allowed
! psql -d regression_invalid_interrupt -c "SELECT 1";

-- to properly drop the database, we need to reset inject fault
SELECT gp_inject_fault('dropdb_before_remove_tablespace', 'reset', dbid)
FROM gp_segment_configuration WHERE content = 0 AND role = 'p';

DROP DATABASE regression_invalid_interrupt;

!\retcode gpconfig -r autovacuum;
!\retcode gpstop -au;
