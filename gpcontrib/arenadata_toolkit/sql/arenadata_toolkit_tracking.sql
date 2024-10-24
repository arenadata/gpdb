-- Tests for size tracking logic introduced in version 1.7
-- start_ignore
\! gpconfig -c shared_preload_libraries -v 'arenadata_toolkit'
\! gpstop -raq -M fast
\! gpconfig -c arenadata_toolkit.tracking_worker_naptime_sec -v '5'
\! gpstop -u
\c
-- end_ignore
-- start_matchsubs
-- m/ERROR:  database \d+ is not tracked/
-- s/\d+/XXX/g
-- end_matchsubs
--start_ignore
DROP DATABASE IF EXISTS tracking1;
--end_ignore
CREATE DATABASE tracking_db1;
\c tracking_db1;
CREATE EXTENSION arenadata_toolkit;

-- 1. Test getting track on not registered database;
SELECT * FROM arenadata_toolkit.tracking_get_track();

SELECT arenadata_toolkit.tracking_register_db();
SELECT pg_sleep(current_setting('arenadata_toolkit.tracking_worker_naptime_sec')::int * 2);

-- 2. Test initial snapshot behaviour. Triggering initial snapshot leads to
-- setting up the bloom filter such that all relfilenodes are considered.
SELECT arenadata_toolkit.tracking_trigger_initial_snapshot();
SELECT is_triggered FROM arenadata_toolkit.is_initial_snapshot_triggered;

-- 3. If user hasn't registered any schema, the default schemas are used.
-- See arenadata_toolkit_guc.c. At commit the bloom filter is cleared. The next
-- call of tracking_get_track() will return nothing if database is not modified in between. 
SELECT count(*) FROM arenadata_toolkit.tracking_get_track();

-- 4. Create table in specific schema and register that schema.
CREATE TABLE arenadata_toolkit.tracking_t1 (i INT)
WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);

SELECT arenadata_toolkit.tracking_register_schema('arenadata_toolkit');

-- Getting the track. Only created table with size 0 is expected;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();

-- 5. Test data extending event. Bloom should capture it.
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,100000);
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();

-- 6. Dropping table. The track shows only relfilenodes without names and other additional info with status 'd'.
DROP TABLE arenadata_toolkit.tracking_t1;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();

-- 8. Test actions on commit and rollback
CREATE TABLE arenadata_toolkit.tracking_t1 (i INT)
WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,100000);

-- If the wrapping transaction rollbacks, the Bloom filter is not cleared up.
BEGIN;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();
ROLLBACK;

-- If commits, filter is cleared.
BEGIN;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();
COMMIT;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();

-- 9. Test repetitive track call within the same transaction. In case of
-- rollback only first changes should be present.
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,10000);
BEGIN;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();

CREATE TABLE arenadata_toolkit.tracking_t2 (j BIGINT) DISTRIBUTED BY (j);
INSERT INTO arenadata_toolkit.tracking_t2 SELECT generate_series(1,10000);
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,10000);

SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();
ROLLBACK;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();

-- 10. Test relkind filtering.
CREATE TABLE arenadata_toolkit.tracking_t1 (i INT)
WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,100000);
CREATE INDEX ON arenadata_toolkit.tracking_t1(i);

-- Want to see index and block dir relation.
SELECT arenadata_toolkit.tracking_register_schema('pg_aoseg');
SELECT arenadata_toolkit.tracking_set_relkinds('o,i');

SELECT  size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tracking_get_track();

DROP TABLE arenadata_toolkit.tracking_t1;

-- Clean up
SELECT arenadata_toolkit.tracking_unregister_db();

\c contrib_regression;
DROP DATABASE tracking_db1;
-- start_ignore
\! gpconfig -r shared_preload_libraries
\! gpconfig -r arenadata_toolkit.tracking_worker_naptime_sec
\! gpstop -u
-- end_ignore
