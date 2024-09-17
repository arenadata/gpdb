-- This test checks for correct output of gp_toolkit.gp_workfile_entries view
-- It is placed in integration tests because testing for this requires checking
-- for workfiles while a query is still running.

-- check there are no workfiles
1: select segid, prefix, slice from gp_toolkit.gp_workfile_entries order by segid;

1: create table workfile_test(id serial, s text) distributed by (id);
1: insert into workfile_test(s) select v::text from generate_series(1, 1000000) v;

1: select gp_inject_fault_infinite('after_workfile_mgr_create_set', 'skip', dbid) from gp_segment_configuration where content > -1 and role = 'p';

1&: select * from workfile_test t1, workfile_test t2, workfile_test t3, workfile_test t4,
    generate_series(1,12);
-- wait until 1 workfile is created on each segment
2: select gp_wait_until_triggered_fault('after_workfile_mgr_create_set', 1, dbid) from gp_segment_configuration where content > -1 and role = 'p';
-- there should be exactly 3 workfiles, two for each segment (no duplication)
2: select segid, prefix, slice from gp_toolkit.gp_workfile_entries order by segid;

-- interrupt the query
2: select gp_inject_fault('create_function_fail', 'panic', dbid) from gp_segment_configuration where role='p' and content > -1;
2: create function my_function() returns void as $$ begin end; $$ language plpgsql;
1<:
1q:

-- check there are no workfiles left
2: select segid, prefix, slice from gp_toolkit.gp_workfile_entries order by segid;
2: drop table workfile_test;
2q: