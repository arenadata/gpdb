-- Test cases when a parallel transaction drops a dependency object
-- while current transaction is yet not committed.

-- Case 1. Function dependency on the schema.
create schema test_1_schema;

1: begin;
1: create function test_1_schema.test_1_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

2&: drop schema test_1_schema;

1: commit;

2<:

1: select test_1_schema.test_1_function();

drop schema test_1_schema cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create schema test_1_schema;
1: begin;
2: begin;
2: drop schema test_1_schema;
1&: create function test_1_schema.test_1_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

2: commit;
1<:
1: end;

-- Case 2. Function dependency on the return type.
create type test_2_type as (a int);

1: begin;
1: create function test_2_function() returns setof test_2_type as $$
    select i from generate_series(1,5)i; /**/
$$ language sql;

2&: drop type test_2_type;

1: commit;

2<:

1: select test_2_function();

drop type test_2_type cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create type test_2_type as (a int);
1: begin;
2: begin;
2: drop type test_2_type;
1&: create function test_2_function() returns setof test_2_type as $$
    select i from generate_series(1,5)i; /**/
$$ language sql;

2: commit;
1<:
1: end;

-- Case 3. Function dependency on the parameter type.
create type test_3_type as enum ('one', 'two');

1: begin;
1: create function test_3_function(a test_3_type) returns text as $$
    select 'Return ' || a; /**/
$$ language sql;

2&: drop type test_3_type;

1: commit;

2<:

1: select test_3_function('one');

drop type test_3_type cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create type test_3_type as enum ('one', 'two');
1: begin;
2: begin;
2: drop type test_3_type;
1&: create function test_3_function(a test_3_type) returns text as $$
    select 'Return ' || a; /**/
$$ language sql;

2: commit;
1<:
1: end;

-- Case 4. Function dependency on the language.
-- start_ignore
drop language if exists plpythonu cascade;
-- end_ignore
create language plpythonu;

1: begin;
1: create function test_4_function() returns text as $$
	return "test"
$$ language plpythonu;

2&: drop language plpythonu;

1: commit;

2<:

1: select test_4_function();

drop language plpythonu cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create language plpythonu;

1: begin;
2: begin;
2: drop language plpythonu;
1&: create function test_4_function() returns text as $$
	return "test"
$$ language plpythonu;

2: commit;
1<:
1: end;

-- Case 5. Function dependency on the parameter default expression.
create function test5_default_value_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

1: begin;
1: create function test_5_function(a text default test5_default_value_function()) returns text as
$$
begin
	return a; /**/
end
$$ language plpgsql;

2&: drop function test5_default_value_function();

1: commit;

2<:

1: select test_5_function();

drop function test5_default_value_function() cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create function test5_default_value_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

1: begin;
2: begin;
2: drop function test5_default_value_function();
1&: create function test_5_function(a text default test5_default_value_function()) returns text as
$$
begin
	return a; /**/
end
$$ language plpgsql;

2: commit;
1<:
1: end;

-- Case 6. Table dependency on the column default expression.
create function test_6_default_value_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

1: begin;
1: create table test_6_table(a text default test_6_default_value_function());

2&: drop function test_6_default_value_function();

1: commit;

2<:

1: insert into test_6_table default values;
1: select * from test_6_table;

drop function test_6_default_value_function() cascade;
drop table test_6_table;

-- Check if dependency is dropped before the creation of the dependent object.
create function test_6_default_value_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

1: begin;
2: begin;
2: drop function test_6_default_value_function();
1&: create table test_6_table(a text default test_6_default_value_function());

2: commit;
1<:
1: end;

-- Case 7. Table dependency on the column type.
create type test_7_type as enum ('one', 'two');

1: begin;
1: create table test_7_table(a test_7_type);

2&: drop type test_7_type;

1: commit;

2<:

1: select * from test_7_table;

drop type test_7_type cascade;
drop table test_7_table;

-- Check if dependency is dropped before the creation of the dependent object.
create type test_7_type as enum ('one', 'two');

1: begin;
2: begin;
2: drop type test_7_type;
1&: create table test_7_table(a test_7_type);

2: commit;
1<:
1: end;

-- Case 8. Table dependency on the collation.
create collation test_8_collation (locale="en_US.utf8");

1: begin;
1: create table test_8_table(a text collate test_8_collation);
1: insert into test_8_table values('data');

2&: drop collation test_8_collation;

1: commit;

2<:

1: select * from test_8_table where a < 'test';

drop collation test_8_collation cascade;
drop table test_8_table;

-- Check if dependency is dropped before the creation of the dependent object.
create collation test_8_collation (locale="en_US.utf8");

1: begin;
2: begin;
2: drop collation test_8_collation;
1&: create table test_8_table(a text collate test_8_collation);

2: commit;
1<:
1: end;
