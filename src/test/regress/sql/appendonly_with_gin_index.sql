-- check that GIN index works with large TID offset (second ctid field) when using AO table
-- Orca performs seq scan in this case, so disable
set optimizer = 0;
set enable_seqscan = 0;
-- collect all tuples on one segment
create temp table t1 (a text, b int default 0) with (appendonly=true) distributed by (b);
insert into t1 select MD5(random()::text) from generate_series(1, 500000);
insert into t1 values ('apple');
create index t1_idx on t1 using gin(to_tsvector('english', a)) with (fastupdate = false);

-- divide block_size by 4 because the size of ItemIdData is 4 bytes
with query as (select a, (ctid::text::point)[1]::int as offset from t1
              where to_tsvector('english', a) @@ to_tsquery('english', 'apple')),
     maxHeapOffset as (select current_setting('block_size')::int/4 as value)
select a from query as q where q.offset > (select * from maxHeapOffset);

explain (costs off)
with query as (select a, (ctid::text::point)[1]::int as offset from t1
              where to_tsvector('english', a) @@ to_tsquery('english', 'apple')),
     maxHeapOffset as (select current_setting('block_size')::int/4 as value)
select a from query as q where q.offset > (select * from maxHeapOffset);

drop table t1;
reset enable_seqscan;
reset optimizer;
