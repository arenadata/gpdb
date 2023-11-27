--
-- DROP OWNED BY for external protocols
--

-- Set up mock external protocol
CREATE OR REPLACE FUNCTION write_to_file()
    RETURNS integer as '$libdir/gpextprotocol.so', 'demoprot_export' LANGUAGE C STABLE NO SQL;
CREATE OR REPLACE FUNCTION read_from_file()
    RETURNS integer as '$libdir/gpextprotocol.so', 'demoprot_import' LANGUAGE C STABLE NO SQL;

CREATE TRUSTED PROTOCOL demo_prot_dob(readfunc = 'read_from_file', writefunc = 'write_to_file');

CREATE ROLE demo_role_dob RESOURCE QUEUE pg_default;

-- Should do nothing, as demo role owns nothing
DROP OWNED BY demo_role_dob;

-- Should drop protocol successfully, since it's not dropped by previous query
DROP PROTOCOL demo_prot_dob;

CREATE TRUSTED PROTOCOL demo_prot_dob(readfunc = 'read_from_file', writefunc = 'write_to_file');

CREATE WRITABLE EXTERNAL TABLE demo_dob_table(a int)
    location('demo_prot_dob://demo_dob_file.txt') format 'text';

-- Make demo role own both protocol and table
ALTER EXTERNAL TABLE demo_dob_table OWNER TO demo_role_dob;
ALTER PROTOCOL demo_prot_dob OWNER TO demo_role_dob;

-- Should drop both table and protocol
DROP OWNED BY demo_role_dob;

-- Both should fail, since they've been dropped with DROP OWNED BY
DROP PROTOCOL demo_prot_dob;
DROP TABLE demo_dob_table;

DROP ROLE demo_role_dob;
