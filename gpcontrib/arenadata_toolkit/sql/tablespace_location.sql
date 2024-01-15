CREATE EXTENSION arenadata_toolkit;

-- Change log level to disable notice messages from PL/pgSQL
SET client_min_messages=WARNING;
SELECT arenadata_toolkit.adb_create_tables();
RESET client_min_messages;

\! mkdir -p /tmp/tblspace_location_test_dir;

CREATE TABLESPACE test_tblspc LOCATION '/tmp/tblspace_location_test_dir';

CREATE TABLE table_at_pg_default(a int)
DISTRIBUTED REPLICATED;

CREATE TABLE table_at_custom_tblspc(a int)
TABLESPACE test_tblspc
DISTRIBUTED REPLICATED;

INSERT INTO table_at_pg_default SELECT generate_series(1,10);
INSERT INTO table_at_custom_tblspc SELECT generate_series(1,10);

SELECT arenadata_toolkit.adb_collect_table_stats();

SELECT
	files.source_table_name,
	files.content,
	files.table_name,
	files.table_tablespace,
	CASE
		WHEN 'pg_default' = files.table_tablespace AND
			 files.tablespace_location = gpconf.datadir || '/base'
		THEN '<SEGMENT_BASE_DIR>/base'
		WHEN 'pg_global' = files.table_tablespace AND
			 files.tablespace_location = gpconf.datadir || '/global'
		THEN '<SEGMENT_BASE_DIR>/global'
		ELSE files.tablespace_location
	END AS tablespace_location
FROM (SELECT '__db_files_current' source_table_name, content, table_name,
	         table_tablespace, tablespace_location, segment_preferred_role::CHAR(1)
	  FROM arenadata_toolkit.__db_files_current
	  WHERE table_name IN ('table_at_pg_default', 'table_at_custom_tblspc',
						   'gp_segment_configuration') -- example of table at pg_global
	  UNION
	  SELECT 'db_files_current' source_table_name, content, table_name,
	         table_tablespace, tablespace_location, segment_preferred_role::CHAR(1)
	  FROM arenadata_toolkit.db_files_current
	  WHERE table_name IN ('table_at_pg_default', 'table_at_custom_tblspc',
						   'gp_segment_configuration') -- example of table at pg_global
	  UNION
	  SELECT 'db_files_history' source_table_name, content, table_name,
	         table_tablespace, tablespace_location, segment_preferred_role::CHAR(1)
	  FROM arenadata_toolkit.db_files_history
	  WHERE table_name IN ('table_at_pg_default', 'table_at_custom_tblspc',
						   'gp_segment_configuration') -- example of table at pg_global
	  ) files
LEFT JOIN gp_segment_configuration gpconf
		  ON gpconf.content = files.content AND
		  gpconf.preferred_role = files.segment_preferred_role
ORDER BY source_table_name, table_name, content;

-- Cleanup
DROP TABLE table_at_pg_default;
DROP TABLE table_at_custom_tblspc;
DROP TABLESPACE test_tblspc;
DROP EXTENSION arenadata_toolkit;
-- Change log level to disable notice messages about dropped objects
-- from "DROP SCHEMA arenadata_toolkit CASCADE;"
SET client_min_messages=WARNING;
DROP SCHEMA arenadata_toolkit CASCADE;
RESET client_min_messages;
\! rm -rf /tmp/tblspace_location_test_dir
