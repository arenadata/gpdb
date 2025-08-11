-- start_ignore
DROP TABLE IF EXISTS heap_table_without_toast;
-- end_ignore

CREATE TABLE heap_table_without_toast(a int, b int)
DISTRIBUTED BY (a);

-- Check with empty tables
SELECT table_name, content, file_size
FROM gp_toolkit.gp_db_files_current
WHERE table_name = 'heap_table_without_toast'
ORDER BY 1, 2;

INSERT INTO heap_table_without_toast SELECT i, i*10 FROM generate_series(1,15) AS i;

-- Check with non-empty tables
SELECT table_name, content, file_size
FROM gp_toolkit.gp_db_files_current
WHERE table_name = 'heap_table_without_toast'
ORDER BY 1, 2;

-- Cleanup
DROP TABLE heap_table_without_toast;
