1: CREATE OR REPLACE FUNCTION show_on_segments(show_name text) RETURNS table (guc_name text, guc_setting text) EXECUTE ON ALL SEGMENTS AS $$ BEGIN	RETURN QUERY SELECT name, setting FROM pg_settings WHERE show_name = name; END; $$ LANGUAGE plpgsql;
1: SHOW autovacuum_naptime;
1: SELECT * FROM show_on_segments('autovacuum_naptime');
1: SHOW autovacuum_vacuum_threshold;
1: SELECT * FROM show_on_segments('autovacuum_vacuum_threshold');
-- test that autovacuum cleans up orphaned temp table correctly

-- speed up test
1: alter system set autovacuum_naptime = 5;
1: alter system set autovacuum_vacuum_threshold = 50;
1: !\retcode gpstop -u;

1: SHOW autovacuum_naptime;
1: SELECT * FROM show_on_segments('autovacuum_naptime');
1: SHOW autovacuum_vacuum_threshold;
1: SELECT * FROM show_on_segments('autovacuum_vacuum_threshold');

1: SELECT * FROM pg_namespace where nspname like 'pg_temp_%' and oid > 16386;
1: SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname like 'pg_temp_%' AND n.oid > 16386;

-- session 1 is going to panic on primary segment 0, creating orphaned temp table on all QEs;
-- session 2 will remain, so the temp table on non-PANIC segment will remain too (until the session resets or exits)
1: create temp table tt_av1(a int);
2: create temp table tt_av2(a int);

-- temp tables created in utility mode: the one on the PANIC segment will be gone, but not other segments.
0U: create temp table ttu_av0(a int);
1U: create temp table ttu_av1(a int);

1: SELECT * FROM pg_namespace where nspname like 'pg_temp_%' and oid > 16386;
1: SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname like 'pg_temp_%' AND n.oid > 16386;

-- Inject a PANIC on one of the segment. Use something that's not going to be hit by background worker (including autovacuum).
1: select gp_inject_fault('create_function_fail', 'panic', dbid) from gp_segment_configuration where content=0 and role='p';
1: create function my_function() returns void as $$ begin end; $$ language plpgsql;

0Uq:
1q:

1: SELECT * FROM pg_namespace where nspname like 'pg_temp_%' and oid > 16386;
1: SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname like 'pg_temp_%' AND n.oid > 16386;

-- make sure the segment restarted,
-- also served as a third test case.
1: create temp table tt_av3(a int);

1: SELECT * FROM pg_namespace where nspname like 'pg_temp_%' and oid > 16386;
1: SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname like 'pg_temp_%' AND n.oid > 16386;

-- clear the fault
1: select gp_inject_fault('create_function_fail', 'reset', dbid) from gp_segment_configuration where content=0 and role='p';

1: SHOW autovacuum_naptime;
1: SELECT * FROM show_on_segments('autovacuum_naptime');
1: SHOW autovacuum_vacuum_threshold;
1: SELECT * FROM show_on_segments('autovacuum_vacuum_threshold');

-- make sure the autovacuum is run at least once,
-- it might've been run already, which is fine.
1: select gp_inject_fault('auto_vac_worker_after_report_activity', 'skip', '', '', 'pg_class', 1, 1, 0, dbid) from gp_segment_configuration where content=0 and role='p';
1: begin;
1: create table bloat_tbl(i int, j int, k int, l int, m int, n int, o int, p int) distributed by (i) partition by range (j) (start (0) end (1000) every (1));
1: abort;
-- wait for autovacuum to hit pg_class, triggering a fault
1: select gp_wait_until_triggered_fault('auto_vac_worker_after_report_activity', 1, dbid) from gp_segment_configuration where content=0 and role='p';
-- clean up fault
1: select gp_inject_fault('auto_vac_worker_after_report_activity', 'reset', dbid) from gp_segment_configuration where content=0 and role='p';

1: SELECT * FROM pg_namespace where nspname like 'pg_temp_%' and oid > 16386;
1: SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname like 'pg_temp_%' AND n.oid > 16386;

-- the orphaned temp table on all QEs should be cleaned up
0U: select count(*) from pg_class where relname = 'tt_av1';
1U: select count(*) from pg_class where relname = 'tt_av1';

-- the temp table that is associated with an existing session should be gone on the PANIC'ed
-- segment (because the QE is killed), but not other segments where QE is still there.
0U: select count(*) from pg_class where relname = 'tt_av2';
1U: select count(*) from pg_class where relname = 'tt_av2';

-- new temp table created is not affected
0U: select count(*) from pg_class where relname = 'tt_av3';
1U: select count(*) from pg_class where relname = 'tt_av3';

-- the utility-mode temp table on the PANIC'ed segment will be gone (because the utility connection
-- was killed), but not other segments where utility connection is still there.
0U: select count(*) from pg_class where relname = 'ttu_av0';
1U: select count(*) from pg_class where relname = 'ttu_av1';

-- restore settings
1: alter system reset autovacuum_naptime;
1: alter system reset autovacuum_vacuum_threshold;
1: !\retcode gpstop -u;

1: SHOW autovacuum_naptime;
1: SELECT * FROM show_on_segments('autovacuum_naptime');
1: SHOW autovacuum_vacuum_threshold;
1: SELECT * FROM show_on_segments('autovacuum_vacuum_threshold');

1: SELECT * FROM pg_namespace where nspname like 'pg_temp_%' and oid > 16386;
1: SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname like 'pg_temp_%' AND n.oid > 16386;

1q:
2q:
0Uq:
1Uq:

1000: SHOW autovacuum_naptime;
1000: SELECT * FROM show_on_segments('autovacuum_naptime');
1000: SHOW autovacuum_vacuum_threshold;
1000: SELECT * FROM show_on_segments('autovacuum_vacuum_threshold');
1000: SELECT * FROM pg_namespace where nspname like 'pg_temp_%' and oid > 16386;
1000: SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname like 'pg_temp_%' AND n.oid > 16386;