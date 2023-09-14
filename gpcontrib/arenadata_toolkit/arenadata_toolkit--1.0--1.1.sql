/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.0--1.1.sql */

ALTER FUNCTION arenadata_toolkit.adb_get_relfilenodes(tablespace_oid OID) ROWS 30000000;

CREATE OR REPLACE VIEW arenadata_toolkit.adb_skew_coefficients
AS
WITH recursive cte AS (
	SELECT
			pgc.oid id,
			pgc.gp_segment_id seg_id,
			arenadata_toolkit.adb_relation_storage_size(pgc.oid) size,
			inh.inhparent parent_id
		FROM gp_dist_random('pg_class') pgc
		LEFT JOIN pg_inherits inh ON inh.inhrelid = pgc.oid
		WHERE pgc.relkind = 'r' AND pgc.relstorage != 'x' AND
			pgc.relnamespace IN (SELECT aunoid FROM gp_toolkit.__gp_user_namespaces)
	UNION ALL
		SELECT cte.parent_id id, cte.seg_id, cte.size, inh.inhparent parent_id
			FROM cte
			LEFT JOIN pg_inherits inh ON inhrelid = cte.parent_id
			WHERE cte.parent_id != 0
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
