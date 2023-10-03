create extension if not exists gp_inject_fault;
create extension if not exists plpython3u;
create or replace procedure find_last_pending_delete_record(wal_name text, data_dir text)
as $$
    import os

    cmd = 'pg_waldump %s -p %s/pg_wal | grep PENDING_DELETE | tail -1' % (wal_name, data_dir)
    res = os.popen(cmd).read()
    if res:
        pos = res.find('PENDING_DELETE')
        plpy.notice(res[pos:])
$$ language plpython3u;

create or replace procedure print_pending_delete()
language plpgsql as $$
declare
    d text;
    w text;
begin
    for d, w in
        select t1.datadir, t2.walname
        from(
            select content, datadir 
            from gp_segment_configuration
            where role = 'p'
        ) t1
        join(
            select gp_segment_id, pg_walfile_name(pg_current_wal_lsn()) walname
            from gp_dist_random('gp_id')
            union all
            select -1, pg_walfile_name(pg_current_wal_lsn())
        ) t2 on t1.content = t2.gp_segment_id
        order by t1.content
    loop
        call find_last_pending_delete_record(w, d);
    end loop;
end $$;

begin;
create table test_ao(i int) with (appendonly=true);
insert into test_ao select generate_series(1, 10000);
select gp_inject_fault_infinite('checkpoint_control_file_updated', 'skip', dbid)
from gp_segment_configuration WHERE role='p';
checkpoint;
select gp_wait_until_triggered_fault('checkpoint_control_file_updated', 1, dbid)
from gp_segment_configuration WHERE role='p';
call print_pending_delete();
create table test_heap(i int);
insert into test_heap select generate_series(1, 10000);
checkpoint;
select gp_wait_until_triggered_fault('checkpoint_control_file_updated', 2, dbid)
from gp_segment_configuration WHERE role='p';
call print_pending_delete();
rollback;

create table test_heap(i int);
insert into test_heap select generate_series(1, 10000);
checkpoint;
select gp_wait_until_triggered_fault('checkpoint_control_file_updated', 3, dbid)
from gp_segment_configuration WHERE role='p';
call print_pending_delete();

select gp_inject_fault_infinite('checkpoint_control_file_updated', 'reset', dbid)
from gp_segment_configuration WHERE role='p';
drop table test_heap;
drop procedure print_pending_delete();
drop procedure find_last_pending_delete_record(text, text);
