/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.0--1.1.sql */

ALTER FUNCTION arenadata_toolkit.adb_get_relfilenodes(tablespace_oid OID) ROWS 30000000;

CREATE OR REPLACE VIEW arenadata_toolkit.adb_skew_coefficients
AS
WITH recursive cte AS (
	WITH all_tables_sizes AS (
		SELECT t1.autrelname relname, t1.autoid id, inhparent parent_id,
				arenadata_toolkit.adb_relation_storage_size(t1.autoid) AS size
			FROM gp_dist_random('gp_toolkit.__gp_user_tables') t1
			LEFT JOIN pg_inherits t2 ON inhrelid = t1.autoid
			WHERE autrelstorage != 'x')
		SELECT *, ROW_NUMBER () OVER (PARTITION BY id
										ORDER BY id) AS seg_id
			FROM all_tables_sizes
	UNION ALL
		SELECT t1.autrelname relname, t1.autoid id, inhparent parent_id, size, seg_id
			FROM gp_toolkit.__gp_user_tables t1
			LEFT JOIN pg_inherits t2 ON inhrelid = t1.autoid
			JOIN cte ON cte.parent_id = t1.autoid
			WHERE autrelstorage != 'x'
	),
	tables_size_by_segments AS (
		SELECT id, sum(size) as size, seg_id
			FROM cte
			GROUP BY (id, seg_id)),
	skew AS (
		SELECT id AS skewoid, stddev(size) AS skewdev, avg(size) AS skewmean
			FROM tables_size_by_segments
			GROUP BY id)
	SELECT
		skew.skewoid AS skcoid,
		pgn.nspname  AS skcnamespace,
		pgc.relname  AS skcrelname,
		CASE WHEN skewdev > 0 THEN skewdev/skewmean * 100.0 ELSE 0 END AS skccoeff
	FROM skew
	JOIN pg_catalog.pg_class pgc ON (skew.skewoid = pgc.oid)
	JOIN pg_catalog.pg_namespace pgn ON (pgc.relnamespace = pgn.oid);

GRANT SELECT ON arenadata_toolkit.adb_skew_coefficients TO public;
