create table table_distr_hashed(a int) distributed by (a);
create table table_distr_hashed_ao_row(a int) with (appendonly=true, orientation=row) distributed by (a);
create table table_distr_hashed_ao_col(a int) with (appendonly=true, orientation=column) distributed by (a);

create table table_distr_random(a int) distributed randomly;
create table table_distr_random_ao_row(a int) with (appendonly=true, orientation=row) distributed randomly;
create table table_distr_random_ao_col(a int) with (appendonly=true, orientation=column) distributed randomly;

create table table_distr_replicated(a int) distributed replicated;
create table table_distr_replicated_ao_row(a int) with (appendonly=true, orientation=row) distributed replicated;
create table table_distr_replicated_ao_col(a int) with (appendonly=true, orientation=column) distributed replicated;

insert into table_distr_hashed select generate_series(1, 20);
insert into table_distr_hashed_ao_row select generate_series(1, 20);
insert into table_distr_hashed_ao_col select generate_series(1, 20);

insert into table_distr_random select generate_series(1, 20);
insert into table_distr_random_ao_row select generate_series(1, 20);
insert into table_distr_random_ao_col select generate_series(1, 20);

insert into table_distr_replicated select generate_series(1, 20);
insert into table_distr_replicated_ao_row select generate_series(1, 20);
insert into table_distr_replicated_ao_col select generate_series(1, 20);

set gp_target_numsegments = 2;

-- Check shrink of old tables
alter table table_distr_hashed rebalance;
alter table table_distr_hashed_ao_row rebalance;
alter table table_distr_hashed_ao_col rebalance;

alter table table_distr_random rebalance;
alter table table_distr_random_ao_row rebalance;
alter table table_distr_random_ao_col rebalance;

alter table table_distr_replicated rebalance;
alter table table_distr_replicated_ao_row rebalance;
alter table table_distr_replicated_ao_col rebalance;

select a, gp_segment_id from table_distr_hashed order by a;
select a, gp_segment_id from table_distr_hashed_ao_row order by a;
select a, gp_segment_id from table_distr_hashed_ao_col order by a;

select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random order by a;
select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random_ao_row order by a;
select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random_ao_col order by a;

select a from table_distr_replicated order by a;
select a from table_distr_replicated_ao_row order by a;
select a from table_distr_replicated_ao_col order by a;

drop table table_distr_hashed;
drop table table_distr_hashed_ao_row;
drop table table_distr_hashed_ao_col;

drop table table_distr_random;
drop table table_distr_random_ao_row;
drop table table_distr_random_ao_col;

drop table table_distr_replicated;
drop table table_distr_replicated_ao_row;
drop table table_distr_replicated_ao_col;

-- Check newly created tables
create table new_table_distr_hashed(a int) distributed by (a);
create table new_table_distr_hashed_ao_row(a int) with (appendonly=true, orientation=row) distributed by (a);
create table new_table_distr_hashed_ao_col(a int) with (appendonly=true, orientation=column) distributed by (a);

create table new_table_distr_random(a int) distributed randomly;
create table new_table_distr_random_ao_row(a int) with (appendonly=true, orientation=row) distributed randomly;
create table new_table_distr_random_ao_col(a int) with (appendonly=true, orientation=column) distributed randomly;

create table new_table_distr_replicated(a int) distributed replicated;
create table new_table_distr_replicated_ao_row(a int) with (appendonly=true, orientation=row) distributed replicated;
create table new_table_distr_replicated_ao_col(a int) with (appendonly=true, orientation=column) distributed replicated;

insert into new_table_distr_hashed select generate_series(1, 20);
insert into new_table_distr_hashed_ao_row select generate_series(1, 20);
insert into new_table_distr_hashed_ao_col select generate_series(1, 20);

insert into new_table_distr_random select generate_series(1, 20);
insert into new_table_distr_random_ao_row select generate_series(1, 20);
insert into new_table_distr_random_ao_col select generate_series(1, 20);

insert into new_table_distr_replicated select generate_series(1, 20);
insert into new_table_distr_replicated_ao_row select generate_series(1, 20);
insert into new_table_distr_replicated_ao_col select generate_series(1, 20);

select a, gp_segment_id from new_table_distr_hashed order by a;
select a, gp_segment_id from new_table_distr_hashed_ao_row order by a;
select a, gp_segment_id from new_table_distr_hashed_ao_col order by a;

select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random order by a;
select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random_ao_row order by a;
select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random_ao_col order by a;

select a from new_table_distr_replicated order by a;
select a from new_table_distr_replicated_ao_row order by a;
select a from new_table_distr_replicated_ao_col order by a;

reset gp_target_numsegments;