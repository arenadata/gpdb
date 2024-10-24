create or replace function get_explain_xml_output(query_string text)
returns xml as
$$
declare
  x xml;
begin
  execute 'explain (format xml) ' || query_string
  into x;
  return x;
end;
$$ language plpgsql;

create or replace function get_motion_snd_recv(query_string text)
returns table(node_name xml, motion_snd xml, motion_recv xml) as
$_$
declare
  node_xml      text := '//*[local-name()="Node-Type"][contains(text(), "Motion")]/../*[local-name()="Node-Type"]/text()';
  motion_snd    text := '//*[local-name()="Node-Type"][contains(text(), "Motion")]/../*[local-name()="Senders"]/text()';
  motion_recv   text := '//*[local-name()="Node-Type"][contains(text(), "Motion")]/../*[local-name()="Receivers"]/text()';
begin
   return query
   execute 'select unnest(xpath(''' || node_xml || ''', x)) node_name,
                   unnest(xpath(''' || motion_snd || ''', x)) motion_snd,
                   unnest(xpath(''' || motion_recv || ''', x)) motion_recv
            from get_explain_xml_output($$' || query_string || '$$) as x';
end;
$_$ language plpgsql;

create or replace function simplified_xml_plan(query_string text)
returns setof text as
$_$
begin
  return query
  select (regexp_matches(x::text, '(.*<(Node-Type|Senders|Receivers|Plan-Rows)>.*)', 'gm'))[1]
  from get_explain_xml_output (query_string) as x;
end;
$_$ language plpgsql;

create table scale_factor_repl(c1 int, c2 int) distributed replicated;
create table scale_factor_distr(c1 int, c2 int) distributed by (c1);
create table scale_factor_rand_distr(c1 int, c2 int) distributed randomly;
create table scale_factor_partitioned (a int) distributed by (a) partition by range(a) (start(1) end(10) every(1));
create table scale_factor_master_only (a int) distributed by (a);

set allow_system_table_mods=true;
delete from gp_distribution_policy where localoid='scale_factor_master_only'::regclass;
reset allow_system_table_mods;

set allow_system_table_mods = on;
create table scale_factor_part_distr(c1 int, c2 int) distributed by(c1);
update gp_distribution_policy set numsegments = 2 where localoid = 'scale_factor_part_distr'::regclass;
reset allow_system_table_mods;

insert into scale_factor_repl select i,i from generate_series(1, 10)i;
insert into scale_factor_distr select i,i from generate_series(1, 10)i;
insert into scale_factor_rand_distr select i,i from generate_series(5, 15)i;
insert into scale_factor_part_distr select i,i from generate_series(1, 10)i;
insert into scale_factor_partitioned values (1), (1), (1);
insert into scale_factor_master_only select generate_series(1, 10);

analyze scale_factor_repl;
analyze scale_factor_distr;
analyze scale_factor_rand_distr;
analyze scale_factor_part_distr;
analyze scale_factor_partitioned;
analyze scale_factor_master_only;

-- This plan from postgres optimizer may seem incorrect at the first glance, but in fact
-- Gather Motion has fractional number of rows, which is 3.3... and this number was rounded up
-- to 4. Also, Hash Semi Join below this motion has the same rows number, but scaled by
-- segments number, which is 3 in our case. That is, we get 1.1 rows and round them to 2.
select simplified_xml_plan($$
  select * from scale_factor_distr where c2 in (select c1/2 from scale_factor_rand_distr limit 3);
$$);

select simplified_xml_plan($$
  select * from scale_factor_repl limit 1;
$$);

select simplified_xml_plan($$
  select * from scale_factor_distr where c1 = 2 or c1 = 5 or c1 = 9;
$$);

select simplified_xml_plan($$
  select count(*) from scale_factor_partitioned where a = 1;
$$);

select simplified_xml_plan($$
  select * from scale_factor_part_distr;
$$);

select * from get_motion_snd_recv($$
  update scale_factor_repl a set c1 = b.c2 from scale_factor_part_distr b returning *;
$$);

select * from get_motion_snd_recv($$
  delete from scale_factor_part_distr a using scale_factor_rand_distr b where b.c1=a.c2;
$$);

select * from get_motion_snd_recv($$
  select t1.c1, row_number() over (order by t1.c1 desc) from scale_factor_distr t1 join scale_factor_distr t2 using(c2);
$$);


drop table scale_factor_repl;
drop table scale_factor_distr;
drop table scale_factor_rand_distr;
drop table scale_factor_part_distr;
drop table scale_factor_partitioned;
drop table scale_factor_master_only;
drop function get_motion_snd_recv(text);
drop function get_explain_xml_output(text);
drop function simplified_xml_plan(text);
