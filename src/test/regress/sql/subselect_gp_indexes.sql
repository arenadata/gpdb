--
-- Test correlated subquery in subplan with motion chooses correct scan type
--
-- Given distributed table
create table subplan_motion_t
(a bigint NOT NULL) DISTRIBUTED BY (a);
-- with constraint
alter table only subplan_motion_t add constraint users_choose_tidscan_pkey primary key (a);
-- and some data
insert into subplan_motion_t
select gen from generate_series(1, 20000) gen;

-- Check that Index Scan is not used
select cte."b" from (select 7 as "b" ) cte
where(select u.a from (select * from subplan_motion_t) u
      where u.a = cte."b") is not null;
-- Plan
explain select cte."b" from (select 7 as "b" ) cte
where(select u.a from (select * from subplan_motion_t) u
      where u.a = cte."b") is not null;

-- Check that TID Scan is not used
select cte."b" from (select 7 as "b" ) cte
where(select u.a from (select * from subplan_motion_t
                             where ctid = (select ctid from subplan_motion_t where a = 7)) u
      where u.a = cte."b") is not null;
-- Plan
explain select cte."b" from (select 7 as "b" ) cte
where(select u.a from (select * from subplan_motion_t
                             where ctid = (select ctid from subplan_motion_t where a = 7)) u
      where u.a = cte."b") is not null;

set enable_indexscan = off;
set enable_bitmapscan = on;

-- Check that Bitmap Scan is not used
select cte."b" from (select 7 as "b" ) cte
where(select u.a from (select * from subplan_motion_t) u
      where u.a = cte."b") is not null;
-- Plan
explain select cte."b" from (select 7 as "b" ) cte
        where(select u.a from (select * from subplan_motion_t) u
              where u.a = cte."b") is not null;

-- start_ignore
drop table if exists subplan_motion_t;
-- end_ignore
