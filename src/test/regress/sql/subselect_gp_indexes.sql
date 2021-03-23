--
-- Test correlated subquery in subplan with motion chooses correct scan type
--
-- Given distributed table
create table subplan_motion_t
(user_id bigint NOT NULL) DISTRIBUTED BY (user_id);
-- with constraint
alter table only subplan_motion_t add constraint users_choose_tidscan_pkey primary key (user_id);
-- and some data
insert into subplan_motion_t
select gen from generate_series(1, 20000) gen;

-- Check that Index Scan is not used
select cte."userId" from (select 7 as "userId" ) cte
where(select u.user_id from (select * from subplan_motion_t) u
      where u.user_id = cte."userId") is not null;
-- Plan
explain select cte."userId" from (select 7 as "userId" ) cte
where(select u.user_id from (select * from subplan_motion_t) u
      where u.user_id = cte."userId") is not null;

-- Check that TID Scan is not used
select cte."userId" from (select 7 as "userId" ) cte
where(select u.user_id from (select * from subplan_motion_t
                             where ctid = (select ctid from subplan_motion_t where user_id = 7)) u
      where u.user_id = cte."userId") is not null;
-- Plan
explain select cte."userId" from (select 7 as "userId" ) cte
where(select u.user_id from (select * from subplan_motion_t
                             where ctid = (select ctid from subplan_motion_t where user_id = 7)) u
      where u.user_id = cte."userId") is not null;

set enable_indexscan = off;
set enable_bitmapscan = on;

-- Check that Bitmap Scan is not used
select cte."userId" from (select 7 as "userId" ) cte
where(select u.user_id from (select * from subplan_motion_t) u
      where u.user_id = cte."userId") is not null;
-- Plan
explain select cte."userId" from (select 7 as "userId" ) cte
        where(select u.user_id from (select * from subplan_motion_t) u
              where u.user_id = cte."userId") is not null;

-- start_ignore
drop table if exists subplan_motion_t;
-- end_ignore
