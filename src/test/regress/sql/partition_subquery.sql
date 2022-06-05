CREATE SCHEMA partition_subquery;
SET SEARCH_PATH=partition_subquery;

-- Given a partition table
CREATE TABLE pt1(id int) DISTRIBUTED BY (id) PARTITION BY RANGE (id) (DEFAULT PARTITION p1);

-- When I run a query, outermost query, and it is selecting FROM a subquery
-- And that subquery, subquery 1, contains another subquery, subquery 2
-- And the outermost query aggregates over a column from an inherited table
-- And the subquery 1 is prevented from being pulled up into a join
SELECT id FROM (
	SELECT id, sum(id) OVER() as sum_id FROM (
		SELECT id FROM pt1
	) as sq1
) as sq2 GROUP BY id;
-- Then, the query executes successfully

--start_ignore
DROP TABLE IF EXISTS pt1;
--end_ignore


-- When a query has a partitioned table and a correlated subquery, it will fail with Query Optimizer
-- We've implemented a fix, and this test is supposed to make sure that this type of queries works correctly
create table t1 (a int) partition by range (a) (start (1) end (3) every (1));
create table t2 (b int8) distributed by (b);

explain select 1 from t1 where a <= (
    SELECT 1 FROM t2
    WHERE t2.b <= (SELECT 1 from t2 as t3 where t3.b = t2.b)
    and t1.a = t2.b);