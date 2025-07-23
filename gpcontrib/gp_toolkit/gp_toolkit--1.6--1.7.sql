/* gpcontrib/gp_toolkit/gp_toolkit--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_toolkit UPDATE TO '1.7" to load this file. \quit

CREATE FUNCTION gp_toolkit.gp_table_size_on_segments(reloid oid)
	RETURNS TABLE (gp_segment_id int, size bigint)
SET search_path = pg_catalog
LANGUAGE SQL EXECUTE ON ALL SEGMENTS AS $$
    SELECT pg_catalog.gp_execution_segment() AS gp_segment_id, *
    FROM pg_catalog.pg_table_size($1)
$$;

GRANT EXECUTE ON FUNCTION gp_toolkit.gp_table_size_on_segments(oid) TO public;

CREATE VIEW gp_toolkit.gp_table_size_skew_coefficients AS
WITH recursive cte AS (
    SELECT
        t.autoid AS id,
        s.gp_segment_id AS seg_id,
        s.size AS size          
    FROM gp_toolkit.__gp_user_data_tables_readable t,
    LATERAL gp_toolkit.gp_table_size_on_segments(t.autoid) AS s
    UNION ALL
    SELECT inhparent AS id, seg_id, size
    FROM cte
    LEFT JOIN pg_catalog.pg_inherits ON inhrelid = id
    WHERE inhparent != 0
), tables_size_by_segments AS (
    SELECT id, sum(size::bigint) AS size
    FROM cte
    GROUP BY id, seg_id
), skew AS (
    SELECT
        id AS skewoid,
        stddev(size) AS skewdev,
        avg(size) AS skewmean
    FROM tables_size_by_segments
    GROUP BY id
)
SELECT
    skew.skewoid AS skcoid,
    pgn.nspname  AS skcnamespace,
    pgc.relname  AS skcrelname,
    CASE WHEN skewdev > 0 THEN skewdev/skewmean * 100.0 ELSE 0 END AS skccoeff
FROM skew
JOIN pg_catalog.pg_class AS pgc ON (skew.skewoid = pgc.oid)
JOIN pg_catalog.pg_namespace AS pgn ON (pgc.relnamespace = pgn.oid);

GRANT SELECT ON TABLE gp_toolkit.gp_table_size_skew_coefficients TO public;

