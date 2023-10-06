-- start_matchsubs
-- m/^ERROR:  Error on receive from .*: server closed the connection unexpectedly/
-- s/^ERROR:  Error on receive from .*: server closed the connection unexpectedly/ERROR: server closed the connection unexpectedly/
--
-- m/^ERROR:  could not stat file.*: No such file or directory/
-- s/^ERROR:  could not stat file.*: No such file or directory/ERROR:  could not stat file: No such file or directory/
-- end_matchsubs

------------------------------------------
create extension if not exists gp_inject_fault;
-- second process can't add node while first is not complete with own adding
select gp_inject_fault('pdl_link_node', 'suspend', dbid) from gp_segment_configuration;
1: begin;
1&: create table pdl_test1(i int);
2: begin;
2&: create table pdl_test2(i int);
select gp_inject_fault('pdl_link_node', 'reset', dbid) from gp_segment_configuration;
1<:
2<:
-- second process can't remove node while first is not complete with removing
select gp_inject_fault('pdl_unlink_node', 'suspend', dbid) from gp_segment_configuration;
1&: rollback;
2&: rollback;
select gp_inject_fault('pdl_unlink_node', 'reset', dbid) from gp_segment_configuration;
1<:
2<:
------------------------------------------

0: create table pdl_paths(id int, path text) distributed replicated;

----------------------------
1: begin;
1: create table test_ao(i int) with (appendonly=true);
1: insert into test_ao select generate_series(1,10000);

1: copy (
    select id, current_setting('data_directory')||'/'||pg_relation_filepath(relid::regclass)
    from (
        select unnest(array[1, 2, 3]) as id, unnest(array[relid, segrelid, visimaprelid]) as relid
        from pg_appendonly
        where relid = 'test_ao'::regclass::oid
    ) t
) to '/tmp/pdl_paths.csv' with csv;

1: copy (select pg_backend_pid()) to '/tmp/pdl_pid.csv' with csv;
!\retcode kill -9 $(cat /tmp/pdl_pid.csv);
1: commit;
1q:
0q:

2: truncate pdl_paths;
2: copy pdl_paths from '/tmp/pdl_paths.csv' with csv;

2: select pg_stat_file(path) from pdl_paths where id = 1;
2: select pg_stat_file(path) from pdl_paths where id = 2;
2: select pg_stat_file(path) from pdl_paths where id = 3;
2q:
----------------------------
0: create or replace function get_full_relpath_on_segments(regclass) returns table(gp_segment_id int, path text)
language sql as $$
    select gp_execution_segment(), current_setting('data_directory')||'/'||pg_relation_filepath($1)
$$ execute on all segments;

1: begin;
1: create table test_ao(i int) with (appendonly=true);
1: insert into test_ao select generate_series(1,10000);

1: copy (
    select t1.id, t2.path
    from(
        select unnest(array[1, 2, 3]) as id, unnest(array[relid, segrelid, visimaprelid]) as relid
        from pg_appendonly
        where relid = 'test_ao'::regclass::oid
    ) t1,
    get_full_relpath_on_segments(t1.relid::regclass) t2
    where t2.gp_segment_id = 0
) to '/tmp/pdl_paths.csv' with csv;

0: !\retcode kill -9 $(ps aux | grep 'postgres' | grep $(psql postgres -Atc "select port from gp_segment_configuration where content=0 and role='p'") | awk '{print $2}');
0q:

1: commit;

1: truncate pdl_paths;
1: copy pdl_paths from '/tmp/pdl_paths.csv' with csv;

1: select pg_stat_file(path) from gp_dist_random('pdl_paths') where gp_execution_segment() = 0 and id = 1;
1: select pg_stat_file(path) from gp_dist_random('pdl_paths') where gp_execution_segment() = 0 and id = 2;
1: select pg_stat_file(path) from gp_dist_random('pdl_paths') where gp_execution_segment() = 0 and id = 3;
----------------------------

0: drop table pdl_paths;
0: drop function get_full_relpath_on_segments;
0: !\retcode rm -f /tmp/pdl_paths.csv;
0: !\retcode rm -f /tmp/pdl_pid.csv;
