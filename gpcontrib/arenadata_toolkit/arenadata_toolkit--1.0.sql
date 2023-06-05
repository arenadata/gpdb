/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION arenadata_toolkit" to load this file. \quit

DO $$
BEGIN
	-- For new deployments create arenadata_toolkit schema, but disconnect it from extension,
	-- since user's tables like arenadata_toolkit.db_files_history need to be out of extension.
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'arenadata_toolkit')
	THEN
		CREATE SCHEMA arenadata_toolkit;
		ALTER EXTENSION arenadata_toolkit DROP SCHEMA arenadata_toolkit;
	END IF;
END$$;

GRANT ALL ON SCHEMA arenadata_toolkit TO public;

/*
 This is part of arenadata_toolkit API for ADB Bundle.
 This function creates tables for performing adb_collect_table_stats.
 */
CREATE FUNCTION arenadata_toolkit.adb_create_tables()
RETURNS VOID
AS $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tables
					WHERE schemaname = 'arenadata_toolkit' AND tablename  = 'db_files_history')
	THEN
		EXECUTE FORMAT($fmt$CREATE TABLE arenadata_toolkit.db_files_history(
			oid BIGINT,
			table_name TEXT,
			table_schema TEXT,
			type CHAR(1),
			storage CHAR(1),
			table_parent_table TEXT,
			table_parent_schema TEXT,
			table_database TEXT,
			table_tablespace TEXT,
			"content" INTEGER,
			segment_preferred_role CHAR(1),
			hostname TEXT,
			address TEXT,
			file TEXT,
			modifiedtime TIMESTAMP WITHOUT TIME ZONE,
			file_size BIGINT,
			collecttime TIMESTAMP WITHOUT TIME ZONE
		)
		WITH (appendonly=true, compresstype=zlib, compresslevel=9)
		DISTRIBUTED RANDOMLY
		PARTITION BY RANGE (collecttime)
		(
			PARTITION %1$I START (date %2$L) INCLUSIVE
			END (date %3$L) EXCLUSIVE
			EVERY (INTERVAL '1 month'),
			DEFAULT PARTITION default_part
		);$fmt$,
		'p' || to_char(NOW(), 'YYYYMM'),
		to_char(NOW(), 'YYYY-MM-01'),
		to_char(NOW() + interval '1 month','YYYY-MM-01'));
		REVOKE ALL ON TABLE arenadata_toolkit.db_files_history FROM public;
	END IF;

	CREATE TABLE IF NOT EXISTS arenadata_toolkit.daily_operation
	(
		schema_name TEXT,
		table_name TEXT,
		action TEXT,
		status TEXT,
		time BIGINT,
		processed_dttm TIMESTAMP
	)
	WITH (appendonly=true, compresstype=zlib, compresslevel=1)
	DISTRIBUTED RANDOMLY;

	REVOKE ALL ON TABLE arenadata_toolkit.daily_operation FROM public;

	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tables
					WHERE schemaname = 'arenadata_toolkit' AND tablename  = 'operation_exclude')
	THEN

		CREATE TABLE arenadata_toolkit.operation_exclude (schema_name TEXT)
		WITH (appendonly=true, compresstype=zlib, compresslevel=1)
		DISTRIBUTED RANDOMLY;

		REVOKE ALL ON TABLE arenadata_toolkit.operation_exclude FROM public;

		INSERT INTO arenadata_toolkit.operation_exclude (schema_name)
		VALUES ('gp_toolkit'),
				('information_schema'),
				('pg_aoseg'),
				('pg_bitmapindex'),
				('pg_catalog'),
				('pg_toast');
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_tables
					WHERE schemaname = 'arenadata_toolkit' AND tablename = 'db_files_current')
	THEN
		CREATE TABLE arenadata_toolkit.db_files_current
		(
			oid BIGINT,
			table_name TEXT,
			table_schema TEXT,
			type CHAR(1),
			storage CHAR(1),
			table_parent_table TEXT,
			table_parent_schema TEXT,
			table_database TEXT,
			table_tablespace TEXT,
			"content" INTEGER,
			segment_preferred_role CHAR(1),
			hostname TEXT,
			address TEXT,
			file TEXT,
			modifiedtime TIMESTAMP WITHOUT TIME ZONE,
			file_size BIGINT
		)
		WITH (appendonly = false) DISTRIBUTED RANDOMLY;
	END IF;

	GRANT SELECT ON TABLE arenadata_toolkit.db_files_current TO public;

	IF EXISTS (SELECT 1 FROM pg_tables
				WHERE schemaname = 'arenadata_toolkit' AND tablename = 'db_files')
	THEN
		DROP EXTERNAL TABLE arenadata_toolkit.db_files;
	END IF;
END$$
LANGUAGE plpgsql VOLATILE
EXECUTE ON MASTER;

CREATE FUNCTION arenadata_toolkit.adb_get_relfilenodes(tablespace_oid OID)
RETURNS TABLE (
	segindex int2,
	dbid int2,
	datoid oid,
	tablespace_oid oid,
	relfilepath text,
	relfilenode oid,
	reloid oid,
	size bigint,
	modified_dttm timestamp without time zone,
	changed_dttm timestamp without time zone
)
EXECUTE ON ALL SEGMENTS
AS '$libdir/arenadata_toolkit', 'adb_get_relfilenodes'
LANGUAGE C STABLE STRICT;

CREATE VIEW arenadata_toolkit.__db_segment_files
AS
SELECT
	segfiles.segindex,
	segfiles.dbid,
	segfiles.datoid,
	segfiles.tablespace_oid,
	gpconf.datadir || '/' || relfilepath AS full_path,
	segfiles.size,
	segfiles.relfilenode,
	gpconf.preferred_role AS segment_preferred_role,
	gpconf.hostname AS hostname,
	gpconf.address AS address,
	segfiles.reloid,
	segfiles.modified_dttm,
	segfiles.changed_dttm
FROM pg_tablespace tbl, arenadata_toolkit.adb_get_relfilenodes(tbl.oid) AS segfiles
		 INNER JOIN gp_segment_configuration AS gpconf
					ON segfiles.dbid = gpconf.dbid;

CREATE VIEW arenadata_toolkit.__db_files_current
AS
SELECT
	c.oid AS oid,
	c.relname AS table_name,
	n.nspname AS table_schema,
	c.relkind AS type,
	c.relstorage AS storage,
	d.datname AS table_database,
	t.spcname AS table_tablespace,
	dbf.segindex AS content,
	dbf.segment_preferred_role AS segment_preferred_role,
	dbf.hostname AS hostname,
	dbf.address AS address,
	dbf.full_path AS file,
	dbf.size AS file_size,
	dbf.modified_dttm AS modifiedtime,
	dbf.changed_dttm AS changedtime
FROM arenadata_toolkit.__db_segment_files dbf
		 LEFT JOIN pg_class AS c
				   ON c.oid = dbf.reloid
		 LEFT JOIN pg_namespace n
				   ON c.relnamespace = n.oid
		 LEFT JOIN pg_tablespace t
				   ON dbf.tablespace_oid = t.oid
		 LEFT JOIN pg_database d
				   ON dbf.datoid = d.oid;

CREATE VIEW arenadata_toolkit.__db_files_current_unmapped
AS
SELECT
	v.table_database,
	v.table_tablespace,
	v.content,
	v.segment_preferred_role,
	v.hostname,
	v.address,
	v.file,
	v.file_size
FROM arenadata_toolkit.__db_files_current v
WHERE v.oid IS NULL;
