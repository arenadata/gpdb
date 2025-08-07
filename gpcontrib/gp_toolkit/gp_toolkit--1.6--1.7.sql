/* gpcontrib/gp_toolkit/gp_toolkit--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_toolkit UPDATE TO '1.7" to load this file. \quit

--------------------------------------------------------------------------------
-- @function:
--        __gp_get_relfilenodes
-- @in:
--        tablespace_oid oid – tablespace oid
--
-- @out:
--        segindex int2 – segment id
--        dbid int2 – db instance id
--        datoid oid – database oid
--        tablespace_oid oid – input tablespace oid
--        relfilepath text – relative file path
--        relfilenode oid – file relfilenode
--        reloid oid – relation oid (null if unmapped)
--        size bigint – file size in bytes
--        modified_dttm timestamp without time zone – time of last modification
--        changed_dttm timestamp without time zone – time of last status change
--
-- @doc:
--        Scan the data directory of the specified tablespace on the current
--        segment and return a row for each regular file that is found.
--
--------------------------------------------------------------------------------
CREATE FUNCTION gp_toolkit.__gp_get_relfilenodes(tablespace_oid oid)
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
LANGUAGE C STRICT AS
'$libdir/gp_toolkit', 'gp_get_relfilenodes';

REVOKE EXECUTE ON FUNCTION gp_toolkit.__gp_get_relfilenodes(oid) FROM PUBLIC;

--------------------------------------------------------------------------------
-- @function:
--        __gp_tablespace_location
-- @in:
--        tablespace_oid oid – tablespace oid
--
-- @out:
--        text – absolute path of the tablespace on the current segment
--
-- @doc:
--        Determine the location for a given tablespace.
--
--------------------------------------------------------------------------------
CREATE FUNCTION gp_toolkit.__gp_tablespace_location(tablespace_oid oid)
RETURNS text
SET search_path = pg_catalog
LANGUAGE SQL STABLE STRICT AS $$
    SELECT CASE
        WHEN spcname = 'pg_default' THEN current_setting('data_directory') || '/base'
        WHEN spcname = 'pg_global'  THEN current_setting('data_directory') || '/global'
        ELSE pg_tablespace_location(tablespace_oid)
    END
    FROM pg_tablespace
    WHERE oid = tablespace_oid;
$$;

REVOKE ALL ON FUNCTION gp_toolkit.__gp_tablespace_location(oid) FROM PUBLIC;

--------------------------------------------------------------------------------
-- @function:
--        gp_get_relfilenodes
-- @in:
--        tablespace_oid oid – tablespace oid
--
-- @out:
--        segindex int2 – segment id
--        dbid int2 – db instance id
--        datoid oid – database oid
--        tablespace_oid oid – input tablespace oid
--        relfilepath text – relative file path
--        relfilenode oid – file relfilenode
--        reloid oid – relation oid (null if unmapped)
--        size bigint – file size in bytes
--        modified_dttm timestamp without time zone – time of last modification
--        changed_dttm timestamp without time zone – time of last status change
--        tablespace_location text – absolute tablespace path
--
-- @doc:
--        Return file metadata from all segments using __gp_get_relfilenodes
--        and add tablespace location for each row. Use planner hint with ROWS
--        to improve query execution.
--
--------------------------------------------------------------------------------
CREATE FUNCTION gp_toolkit.gp_get_relfilenodes(tablespace_oid oid)
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
    changed_dttm timestamp without time zone,
    tablespace_location text
)
SET search_path = pg_catalog
LANGUAGE SQL EXECUTE ON ALL SEGMENTS STRICT 
ROWS 30000000 AS $$
    WITH l AS (SELECT gp_toolkit.__gp_tablespace_location($1) AS tablespace_location)
    SELECT r.*, l.tablespace_location 
    FROM gp_toolkit.__gp_get_relfilenodes($1) AS r, l;
$$;

REVOKE ALL ON FUNCTION gp_toolkit.gp_get_relfilenodes(oid) FROM PUBLIC;

--------------------------------------------------------------------------------
-- @view:
--        gp_db_segment_files
--
-- @out:
--        segindex int2 – segment id
--        dbid int2 – db instance id
--        datoid oid – database oid
--        tablespace_oid oid – tablespace oid
--        full_path text – absolute file path
--        size bigint – file size in bytes
--        relfilenode oid – file relfilenode
--        segment_preferred_role text – preferred role of the segment
--        hostname text – host where the segment resides
--        address text – network address used to reach the segment
--        reloid oid – relation oid (null if unmapped)
--        modified_dttm timestamp without time zone – time of last modification
--        changed_dttm timestamp without time zone – time of last status change
--        tablespace_location text – absolute tablespace path
--
-- @doc:
--        Return file metadata from segments. Joins gp_get_relfilenodes with 
--        gp_segment_configuration to add segment host and role info.
--
--------------------------------------------------------------------------------
CREATE VIEW gp_toolkit.gp_db_segment_files AS
SELECT
    segfiles.segindex AS segindex,
    segfiles.dbid AS dbid,
    segfiles.datoid AS datoid,
    segfiles.tablespace_oid AS tablespace_oid,
    gpconf.datadir || '/' || segfiles.relfilepath AS full_path,
    segfiles.size AS size,
    segfiles.relfilenode AS relfilenode,
    gpconf.preferred_role AS segment_preferred_role,
    gpconf.hostname AS hostname,
    gpconf.address AS address,
    segfiles.reloid AS reloid,
    segfiles.modified_dttm AS modified_dttm,
    segfiles.changed_dttm AS changed_dttm,
    segfiles.tablespace_location AS tablespace_location
FROM pg_tablespace, LATERAL gp_toolkit.gp_get_relfilenodes(pg_tablespace.oid) AS segfiles
    JOIN gp_segment_configuration AS gpconf ON segfiles.dbid = gpconf.dbid;

REVOKE ALL ON gp_toolkit.gp_db_segment_files FROM PUBLIC;

--------------------------------------------------------------------------------
-- @view:
--        gp_db_files_current
--
-- @out:
--        oid oid – table oid (null if unmapped) 
--        table_name name – table name
--        table_schema name – schema name
--        type char – table type (relkind)
--        relamname name – access method
--        relispartition boolean – true if the table is a partition
--        table_database name – database name
--        table_tablespace name – tablespace name
--        content int2 – segment content id
--        segment_preferred_role text – preferred role of the segment
--        hostname text – host where the file is located
--        address text – network address of the segment
--        file text – absolute file path
--        file_size bigint – file size in bytes
--        modifiedtime timestamp without time zone – time of last modification
--        changedtime timestamp without time zone – time of last status change
--        tablespace_location text – absolute tablespace path
--
-- @doc:
--        Return data from gp_db_segment_files and add related metadata
--        from pg_class, pg_namespace, pg_database, and pg_tablespace.
--
--------------------------------------------------------------------------------
CREATE VIEW gp_toolkit.gp_db_files_current AS
SELECT
    c.oid AS oid,
    c.relname AS table_name,
    n.nspname AS table_schema,
    c.relkind AS type,
    a.amname AS relamname,
    c.relispartition as relispartition,
    d.datname AS table_database,
    t.spcname AS table_tablespace,
    dbf.segindex AS content,
    dbf.segment_preferred_role AS segment_preferred_role,
    dbf.hostname AS hostname,
    dbf.address AS address,
    dbf.full_path AS file,
    dbf.size AS file_size,
    dbf.modified_dttm AS modifiedtime,
    dbf.changed_dttm AS changedtime,
    dbf.tablespace_location AS tablespace_location
FROM gp_toolkit.gp_db_segment_files AS dbf
LEFT JOIN pg_class c ON c.oid = dbf.reloid
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_am a ON a.oid = c.relam
LEFT JOIN pg_database d ON d.oid = dbf.datoid
LEFT JOIN pg_tablespace t ON t.oid = dbf.tablespace_oid;

REVOKE ALL ON gp_toolkit.gp_db_files_current FROM PUBLIC;

--------------------------------------------------------------------------------
-- @view:
--        gp_db_files_current_unmapped
--
-- @out:
--        table_database name – database name
--        table_tablespace name – tablespace name
--        content int2 – segment content id
--        segment_preferred_role text – preferred role of the segment
--        hostname text – host where the file is located
--        address text – network address of the segment
--        file text – absolute file path
--        file_size bigint – file size in bytes
--        tablespace_location text – absolute tablespace path
--
-- @doc:
--        Subset of gp_db_files_current that lists data files with no matching
--        pg_class entry (orphaned or transient files).
--
--------------------------------------------------------------------------------
CREATE VIEW gp_toolkit.gp_db_files_current_unmapped AS
SELECT
    v.table_database,
    v.table_tablespace,
    v.content,
    v.segment_preferred_role,
    v.hostname,
    v.address,
    v.file,
    v.file_size,
    v.tablespace_location
FROM gp_toolkit.gp_db_files_current v
WHERE v.oid IS NULL;

REVOKE ALL ON gp_toolkit.gp_db_files_current_unmapped FROM PUBLIC;
