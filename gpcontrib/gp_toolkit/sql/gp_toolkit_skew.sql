-- start_ignore
DROP EXTENSION IF EXISTS gp_toolkit;
DROP SCHEMA IF EXISTS gp_toolkit CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mat_view;
DROP TABLE IF EXISTS heap, ao, co, heap_usr, heap_ts, ao_ts, co_ts, part_table;
DROP EXTERNAL TABLE IF EXISTS ext_table;
REVOKE CREATE, USAGE ON SCHEMA public FROM user_test;
DROP ROLE IF EXISTS user_test;
DROP TABLESPACE IF EXISTS ts_test;
\! rm -f @abs_srcdir@/data/test_file.csv/test_file.csv
-- end_ignore

CREATE EXTENSION gp_toolkit;

-- test of general behavior

CREATE TABLE heap(i int, j int) WITH (APPENDONLY = false) DISTRIBUTED BY(i);
CREATE TABLE ao  (i int, j int) WITH (APPENDONLY = true) DISTRIBUTED BY(i);
CREATE TABLE co  (i int, j int) WITH (APPENDONLY = true, ORIENTATION = column) DISTRIBUTED BY(i);

INSERT INTO heap SELECT i, i AS j FROM generate_series(1, 100000) i;
INSERT INTO ao   SELECT i, i AS j FROM generate_series(1, 100000) i;
INSERT INTO co   SELECT i, i AS j FROM generate_series(1, 100000) i;

-- external table
\! echo "1|101" >  @abs_srcdir@/data/test_file.csv
\! echo "2|102" >> @abs_srcdir@/data/test_file.csv
\! echo "3|103" >> @abs_srcdir@/data/test_file.csv
CREATE EXTERNAL TABLE ext_table ( i int, j int)
	LOCATION ('file://@hostname@@abs_srcdir@/data/test_file.csv')
    FORMAT 'csv' (DELIMITER '|');
SELECT * FROM ext_table;

-- materialized view
CREATE MATERIALIZED VIEW mat_view AS SELECT * FROM heap DISTRIBUTED BY(i);

-- round is used to prevent test flakiness. Original values of coefficient for
-- AO, CO, heap tables  differs (but for AO/CO them are close enough), because
-- of different storage models. (Also zero value of skew coefficient for heap
-- table is a kind of luck conditioned by NUM_SEG=3 and INSERTed 100000 tuples)
SELECT skcnamespace, skcrelname, round(skccoeff, 2) as skccoeff_round
FROM gp_toolkit.gp_table_size_skew_coefficients ORDER BY skcrelname;

-- produce the skew
INSERT INTO heap SELECT 1, i AS j FROM generate_series(1, 10000) i;
INSERT INTO ao   SELECT 1, i AS j FROM generate_series(1, 10000) i;
INSERT INTO co   SELECT 1, i AS j FROM generate_series(1, 10000) i;
refresh materialized view mat_view;

-- check that skewcoeff was increased
SELECT skcnamespace, skcrelname, round(skccoeff, 2) as skccoeff_round
FROM gp_toolkit.gp_table_size_skew_coefficients ORDER BY skcrelname;

-- test work with another role

CREATE ROLE user_test;
GRANT CREATE, USAGE ON SCHEMA public TO user_test;
ALTER USER user_test SET search_path TO public;

SET ROLE user_test;
CREATE TABLE heap_usr(i int, j int) WITH (APPENDONLY = false) DISTRIBUTED BY(i);
INSERT INTO heap_usr SELECT i, i AS j FROM generate_series(1, 100000) i;
INSERT INTO heap_usr SELECT 1, i AS j FROM generate_series(1, 10000) i;

-- should see only heap_usr, since the rest shouldn't be available for user_test
SELECT skcnamespace, skcrelname, round(skccoeff, 2) as skccoeff_round
FROM gp_toolkit.gp_table_size_skew_coefficients ORDER BY skcrelname;

RESET ROLE;
DROP EXTERNAL TABLE ext_table;
DROP MATERIALIZED VIEW mat_view;
DROP TABLE heap, ao, co;
\! rm /home/gpadmin/.data/test_file.csv

-- should also see heap_usr, since it's a superuser role
SELECT skcnamespace, skcrelname, round(skccoeff, 2) as skccoeff_round
FROM gp_toolkit.gp_table_size_skew_coefficients ORDER BY skcrelname;

DROP TABLE heap_usr;
REVOKE CREATE, USAGE ON SCHEMA public FROM user_test;
DROP ROLE user_test;

-- test work with custom tablespace

\! mkdir /home/gpadmin/.data/test
CREATE TABLESPACE ts_test LOCATION '@testtablespace@';

CREATE TABLE heap_ts(i int, j int) WITH (APPENDONLY = false) TABLESPACE ts_test DISTRIBUTED BY(i);
CREATE TABLE ao_ts (i int, j int) WITH (APPENDONLY = true) TABLESPACE ts_test DISTRIBUTED BY(i);
CREATE TABLE co_ts (i int, j int) WITH (APPENDONLY = true, ORIENTATION = column) TABLESPACE ts_test DISTRIBUTED BY(i);

INSERT INTO heap_ts SELECT i, i AS j FROM generate_series(1, 100000) i;
INSERT INTO ao_ts   SELECT i, i AS j FROM generate_series(1, 100000) i;
INSERT INTO co_ts   SELECT i, i AS j FROM generate_series(1, 100000) i;

SELECT skcnamespace, skcrelname, round(skccoeff, 2) as skccoeff_round
FROM gp_toolkit.gp_table_size_skew_coefficients ORDER BY skcrelname;

DROP TABLE heap_ts, ao_ts, co_ts;
DROP TABLESPACE ts_test;
\! rm -r /home/gpadmin/.data/test

-- test with partitioned table

CREATE TABLE part_table (id int, a int, b int, c int, d int, str text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (a)
	SUBPARTITION BY RANGE (b)
		SUBPARTITION TEMPLATE (START (1) END (3) EVERY (1))
	SUBPARTITION BY RANGE (c)
		SUBPARTITION TEMPLATE (START (1) END (3) EVERY (1))
	SUBPARTITION BY RANGE (d)
		SUBPARTITION TEMPLATE (START (1) END (3) EVERY (1))
	SUBPARTITION BY LIST (str)
		SUBPARTITION TEMPLATE (
			SUBPARTITION sub_prt1 VALUES ('sub_prt1'),
			SUBPARTITION sub_prt2 VALUES ('sub_prt2'))
	(START (1) END (3) EVERY (1));

-- check that adb_skew_coefficients works on empty table
SELECT skcnamespace, skcrelname, round(skccoeff, 2) AS skccoeff_round
	FROM gp_toolkit.gp_table_size_skew_coefficients
	WHERE skcrelname LIKE 'part_table%'
	ORDER BY skcrelname;

-- add small data to all parts of table
INSERT INTO part_table
	SELECT i+1, mod(i/16,2)+1, mod(i/8,2)+1, mod(i/4,2)+1, mod(i/2,2)+1, 'sub_prt' || mod(i,2)+1
	FROM generate_series(0,399) as i;

SELECT skcnamespace, skcrelname, round(skccoeff, 2) AS skccoeff_round
	FROM gp_toolkit.gp_table_size_skew_coefficients
	WHERE skcrelname LIKE 'part_table%'
	ORDER BY skcrelname;

-- add a lot of data for one part to generate big skew coefficient
-- distributing by first column is helps to us to put all data to one segment
INSERT INTO part_table SELECT 1,1,1,1,1,'sub_prt1' FROM generate_series(1,10000) AS i;

SELECT skcnamespace, skcrelname, round(skccoeff, 2) AS skccoeff_round
	FROM gp_toolkit.gp_table_size_skew_coefficients
	WHERE skcrelname LIKE 'part_table%' AND skccoeff != 0
	ORDER BY skcrelname;

DROP TABLE part_table;

DROP EXTENSION gp_toolkit;
DROP SCHEMA gp_toolkit CASCADE;
