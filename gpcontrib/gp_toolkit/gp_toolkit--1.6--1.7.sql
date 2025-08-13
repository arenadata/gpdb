/* gpcontrib/gp_toolkit/gp_toolkit--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_toolkit UPDATE TO '1.7" to load this file. \quit

--------------------------------------------------------------------------------
-- @function:
--        gp_table_size_on_segments
--
-- @in:
--        reloid oid - oid of the table to get sizes for
--
-- @out:
--        gp_segment_id int - segment id
--        size bigint - size in bytes
--
-- @doc:
--        Returns disk space used by the specified table on each segment,
--        excluding indexes but including TOAST, free‑space map (FSM) and
--        visibility map. Even an empty table can therefore report a
--        non‑zero size.
--
--------------------------------------------------------------------------------
CREATE FUNCTION gp_toolkit.gp_table_size_on_segments(reloid oid)
RETURNS TABLE (gp_segment_id int, size bigint)
SET search_path = pg_catalog
LANGUAGE SQL EXECUTE ON ALL SEGMENTS AS $$
    SELECT gp_execution_segment() AS gp_segment_id, *
    FROM pg_table_size($1)
$$;

GRANT EXECUTE ON FUNCTION gp_toolkit.gp_table_size_on_segments(oid) TO public;

--------------------------------------------------------------------------------
-- @view:
--        gp_table_size_skew_coefficients
--
-- @out:
--        skcoid oid - oid of the table
--        skcnamespace name - schema name
--        skcrelname name - table name
--        skccoeff numeric - coefficient of variation for data distribution
--
-- @doc:
--        Calculates data‑distribution skew by computing the coefficient of
--        variation for physical table size across segments. Works for
--        ordinary, partitioned tables and materialized views. For non‑leaf
--        partitions the segment size is derived from their leaf children.
--
--------------------------------------------------------------------------------
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
    LEFT JOIN pg_inherits ON inhrelid = id
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
JOIN pg_class AS pgc ON (skew.skewoid = pgc.oid)
JOIN pg_namespace AS pgn ON (pgc.relnamespace = pgn.oid);

GRANT SELECT ON TABLE gp_toolkit.gp_table_size_skew_coefficients TO public;

