/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.2--1.3.sql */

/*
 * Returns column pair (table_schema, table_name) ordered by haven't been vacuumed
 * before at the head of the list and vacuumed before by ascending order (default strategy)
 */
CREATE FUNCTION arenadata_toolkit.adb_vacuum_strategy_newest_first(actionname TEXT)
RETURNS TABLE (table_schema NAME, table_name NAME) AS
$$
BEGIN
	RETURN query
	SELECT n.nspname AS table_schema, c.relname AS table_name
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
	LEFT JOIN pg_stat_last_operation o ON o.classid = 'pg_class'::regclass::oid AND o.objid = c.oid AND o.staactionname = actionname
	LEFT JOIN pg_partition_rule p ON p.parchildrelid = c.oid
	WHERE c.relkind = 'r'
		AND c.relstorage != 'x'
		AND n.nspname NOT IN (SELECT schema_name FROM arenadata_toolkit.operation_exclude)
		AND p.parchildrelid IS NULL
	ORDER BY o.statime ASC NULLS FIRST;
END;
$$ LANGUAGE plpgsql EXECUTE ON MASTER;

/*
 * Only for admin usage.
 */
REVOKE ALL ON FUNCTION arenadata_toolkit.adb_vacuum_strategy_newest_first(actionname TEXT) FROM public;

/*
 * Returns column pair (table_schema, table_name) ordered by haven't been vacuumed
 * before at the end of the list and vacuumed before by ascending order
 */
CREATE FUNCTION arenadata_toolkit.adb_vacuum_strategy_newest_last(actionname TEXT)
RETURNS TABLE (table_schema NAME, table_name NAME) AS
$$
BEGIN
	RETURN query
    SELECT n.nspname AS table_schema, c.relname AS table_name
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
	LEFT JOIN pg_stat_last_operation o ON o.classid = 'pg_class'::regclass::oid AND o.objid = c.oid AND o.staactionname = actionname
	LEFT JOIN pg_partition_rule p ON p.parchildrelid = c.oid
	WHERE c.relkind = 'r'
		AND c.relstorage != 'x'
		AND n.nspname NOT IN (SELECT schema_name FROM arenadata_toolkit.operation_exclude)
		AND p.parchildrelid IS NULL
	ORDER BY o.statime ASC NULLS LAST;
END;
$$ LANGUAGE plpgsql EXECUTE ON MASTER;

/*
 * Only for admin usage.
 */
REVOKE ALL ON FUNCTION arenadata_toolkit.adb_vacuum_strategy_newest_last(actionname TEXT) FROM public;
