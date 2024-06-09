-- start_ignore
-- Cleanup before test
DROP TABLE IF EXISTS dstr_repl_tst, union_table, dstr_repl_tst_stg;
DROP FUNCTION IF EXISTS MergeToEcom();
-- end_ignore

-- create replicated table
CREATE TABLE dstr_repl_tst(DivisionID INT primary key, DivisionName VARCHAR(100))
DISTRIBUTED REPLICATED;

-- create 'staging table'
CREATE TABLE dstr_repl_tst_stg(like dstr_repl_tst
including defaults
including indexes
including comments);

INSERT INTO dstr_repl_tst VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
INSERT INTO dstr_repl_tst_stg VALUES (3, 'III'), (4, 'IV'), (5, 'V'), (6, 'VI'), (7, 'VII');

-- create merger function
CREATE OR REPLACE FUNCTION MergeToEcom(OUT u INT, OUT i INT) RETURNS record as
$$ BEGIN

    WITH updated AS (
        UPDATE dstr_repl_tst d
        SET DivisionName = s.DivisionName
        FROM dstr_repl_tst_stg s
        WHERE d.DivisionID = s.DivisionID
        returning 1)
    SELECT count(*) into u from updated;
    i := 0;

END $$ language plpgsql;

-- try to merge
-- There was error: "consistency check on SPI tuple count failed".
-- Now, it have to work correctly
SELECT * FROM MergeToEcom();

-- Check "SELECT INTO" with replicated table
SELECT t1.DivisionID AS divisionID,
       t1.DivisionName AS DivisionName1,
       t2.DivisionName AS DivisionName2
INTO union_table
FROM dstr_repl_tst t1
JOIN dstr_repl_tst_stg t2 ON t1.DivisionID = t2.DivisionID;

-- Check the result
SELECT * from union_table  ORDER BY 1;

DROP TABLE dstr_repl_tst, union_table, dstr_repl_tst_stg;
DROP FUNCTION MergeToEcom();

-- Check, that INSERT produce the error with correct explanation
DO $$
DECLARE
    res integer;
BEGIN
    CREATE TEMPORARY TABLE t_test(a integer) DISTRIBUTED REPLICATED;
    INSERT INTO t_test(a) VALUES (1), (2), (3) RETURNING a INTO res;
    RAISE NOTICE '%', res;
    DROP TABLE t_test;
END;
$$;

-- Check, that INSERT will be executed without an error
DO $$
DECLARE
    res integer;
BEGIN
    CREATE TEMPORARY TABLE t_test(a integer) DISTRIBUTED REPLICATED;
    INSERT INTO t_test(a) VALUES (1) RETURNING a INTO res;
    RAISE NOTICE '%', res;
    DROP TABLE t_test;
END;
$$;
