SET client_min_messages=WARNING;

-- start_matchsubs
-- m/(pg_toast|pg_aovisimap|pg_aoseg)_\d+(.*)?/
-- s/(pg_toast|pg_aovisimap|pg_aoseg)_\d+(.*)?/$1_10000$2/
-- m/        .*table_name       .*/
-- s/        .*table_name       .*/        table_name       /
-- end_matchsubs

\! mkdir -p /tmp/arenadata_toolkit_test
CREATE TABLESPACE arenadata_test location '/tmp/arenadata_toolkit_test/';

CREATE EXTENSION arenadata_toolkit;
SELECT arenadata_toolkit.adb_create_tables();

-- Test work with empty tablespace
SELECT table_schema, table_tablespace, content, table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;

-- Test work with non-empty tablespace

-- Simple table
CREATE TABLE arenadata_toolkit_table(a int, b int)
	TABLESPACE arenadata_test
	DISTRIBUTED BY (a);
SELECT table_schema, table_tablespace, content, table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;
DROP TABLE arenadata_toolkit_table;

-- Table with toasts
CREATE TABLE arenadata_toolkit_table(a int, b text)
	TABLESPACE arenadata_test
	DISTRIBUTED BY (a);
SELECT table_schema, table_tablespace, content, table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;
DROP TABLE arenadata_toolkit_table;

-- AO table
-- TODO: column "table_name" was removed from output because of variable length
-- at this names: oid can be with different length and width of column can be
-- changed. This cannot be fixed using matchsubs.
CREATE TABLE arenadata_toolkit_table(a int, b int)
	WITH (APPENDONLY=true)
	TABLESPACE arenadata_test
	DISTRIBUTED BY (a);
SELECT table_schema, table_tablespace, content
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;
DROP TABLE arenadata_toolkit_table;

-- Work with Temp table should be as at empty tablespace
CREATE TEMP TABLE arenadata_toolkit_table(a int, b int)
	TABLESPACE arenadata_test
	DISTRIBUTED BY (a);
SELECT table_schema, table_tablespace, content, table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;
DROP TABLE arenadata_toolkit_table;

-- Check, that there are files at filesystem of tablespace after dropping tables,
-- which do not apply to tables at tablespace
SELECT table_schema, table_tablespace, content, table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test'
	ORDER BY
		table_name ASC,
		content ASC;

-- Cleanup
DROP TABLESPACE arenadata_test;
\! rm -rf /tmp/arenadata_toolkit_test

DROP EXTENSION arenadata_toolkit;
DROP SCHEMA arenadata_toolkit cascade;

RESET client_min_messages;
