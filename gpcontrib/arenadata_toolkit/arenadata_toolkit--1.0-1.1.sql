/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.0-1.1.sql */

CREATE FUNCTION arenadata_toolkit.get_full_storage_size()
RETURNS TABLE (
	autoid OID,
	size BIGINT,
	seg_id BIGINT)
AS $$
DECLARE
	r int;
BEGIN

	EXECUTE FORMAT($fmt$
		DROP TABLE IF EXISTS __adb_full_storage;
	$fmt$);

	EXECUTE FORMAT($fmt$
		CREATE TEMPORARY TABLE __adb_full_storage AS
			WITH
				tmp_size AS (
					SELECT
							autoid,
							arenadata_toolkit.adb_relation_storage_size(autoid) AS size
						FROM gp_dist_random('gp_toolkit.__gp_user_tables')
						WHERE autrelstorage != 'x')
				SELECT *,
						ROW_NUMBER () OVER (
									PARTITION BY autoid
									ORDER BY autoid) AS seg_id
					FROM tmp_size;
	$fmt$);

	FOR r IN 1..3 LOOP
		EXECUTE FORMAT($fmt$
			UPDATE __adb_full_storage AS updating_table SET
				size = (COALESCE((WITH tmp AS
									(SELECT * FROM __adb_full_storage AS filtered_table
											WHERE filtered_table.seg_id = updating_table.seg_id)
								SELECT sum((SELECT size
												FROM tmp
												WHERE tmp.autoid = pg_inherits.inhrelid))
									FROM pg_inherits
									WHERE pg_inherits.inhparent = updating_table.autoid), 0))
				WHERE updating_table.size = 0;
		$fmt$);
	END LOOP;

	RETURN QUERY SELECT * FROM __adb_full_storage;

-- Cleanup:
	EXECUTE FORMAT($fmt$
		DROP TABLE IF EXISTS __adb_full_storage;
	$fmt$);
END$$
LANGUAGE plpgsql VOLATILE
EXECUTE ON MASTER;

/*
 Only for admin usage.
 */
REVOKE ALL ON FUNCTION arenadata_toolkit.get_full_storage_size() FROM public;

CREATE OR REPLACE VIEW arenadata_toolkit.adb_skew_coefficients
AS
WITH skew AS (
	SELECT
		autoid AS skewoid,
		stddev(size) AS skewdev,
		avg(size) AS skewmean
	FROM arenadata_toolkit.get_full_storage_size() GROUP BY autoid
)
SELECT
	skew.skewoid AS skcoid,
	pgn.nspname  AS skcnamespace,
	pgc.relname  AS skcrelname,
	CASE WHEN skewdev > 0 THEN skewdev/skewmean * 100.0 ELSE 0 END AS skccoeff
FROM skew
JOIN pg_catalog.pg_class pgc
	ON (skew.skewoid = pgc.oid)
JOIN pg_catalog.pg_namespace pgn
	ON (pgc.relnamespace = pgn.oid);

GRANT SELECT ON arenadata_toolkit.adb_skew_coefficients TO public;
