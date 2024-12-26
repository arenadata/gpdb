-- start_ignore
! gpconfig -c runaway_detector_activation_percent -v 10;
! gpstop -rai;

drop table if exists t1;
drop role if exists test;
drop resource group test_group;
-- end_ignore

create resource group test_group with (cpu_rate_limit=20, memory_limit=15, memory_shared_quota=100, memory_spill_ratio=0);

create role test with resource group test_group;

set role test;
create table t1 (a int) distributed by (a);
insert into t1 select a from generate_series(1, 10) a;
alter table t1 set distributed randomly;

-- Test that the starting memory is visible to the resource group.
1: set role test;
1&: select count(*) from t1 where pg_sleep(1) is not null;

2: select segment, mem.* from gp_toolkit.gp_resgroup_status, json_object_keys(memory_usage)
    as segment, json_to_record(memory_usage -> segment) mem (used int) where rsgname = 'test_group';
1<:
1q:
2q:

-- The runaway detector test. A query with a large number of slices should
-- be terminated due to high memory consumption.
select count(*) from t1 a1
                join t1 a2 using(a)
                join t1 a3 using(a)
                join t1 a4 using(a)
                join t1 a5 using(a)
                join t1 a6 using(a)
                join t1 a7 using(a)
                join t1 a8 using(a)
                join t1 a9 using(a)
                join t1 a10 using(a);

drop table t1;
reset role;
drop role test;
drop resource group test_group;
-- start_ignore
! gpconfig -c runaway_detector_activation_percent -v 100;
! gpstop -rai;
-- end_ignore
