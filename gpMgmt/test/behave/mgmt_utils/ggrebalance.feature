@ggrebalance
Feature: ggrebalance behave tests

    @demo_cluster
    Scenario: test ggrebalance simple scenarious
        Given the database is running
        When the user runs "ggrebalance -x 3"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Target segment count (3) >= current segment count (3)." to logfile with latest timestamp
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Cleanup is not required." to logfile with latest timestamp
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Cleanup is not required." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -x 2"
        Then ggrebalance should return a return code of 0
        And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
        When the user runs "ggrebalance -x 1"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Previous run was completed successfully. Please execute cleanup before a new run." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Previous run was completed successfully. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Cleanup is complete" to logfile with latest timestamp

#    Scenario Outline: ggrebalance - check continue after interrupted state
#        Given the database is not running
#         And a working directory of the test as '/data/gpdata/ggrebalance'
#         And a cluster is created with mirrors on "cdw" and "sdw1"
#         And set fault inject "<fault_name>"
#         And database "test_db_1" exists
#         And the user runs psql with "-c 'CREATE SCHEMA test_schema_1'" against database "test_db_1"
#         And the user runs psql with "-c 'CREATE TABLE test_schema_1.test_table_1 (a int) DISTRIBUTED BY(a)'" against database "test_db_1"
#         And the user runs psql with "-c 'INSERT INTO test_schema_1.test_table_1 SELECT generate_series(1, 100)'" against database "test_db_1"
#         And the user runs psql with "-c 'CREATE TABLE test_schema_1.test_table_2 (a int) DISTRIBUTED BY(a)'" against database "test_db_1"
#         And the user runs psql with "-c 'INSERT INTO test_schema_1.test_table_2 SELECT generate_series(1, 100)'" against database "test_db_1"
#         And database "test_db_2" exists
#         And the user runs psql with "-c 'CREATE SCHEMA test_schema_2'" against database "test_db_2"
#         And the user runs psql with "-c 'CREATE TABLE test_schema_2.test_table_1 (a int) DISTRIBUTED BY(a)'" against database "test_db_2"
#         And the user runs psql with "-c 'INSERT INTO test_schema_2.test_table_1 SELECT generate_series(1, 100)'" against database "test_db_2"
#         And the user runs psql with "-c 'CREATE TABLE test_schema_2.test_table_2 (a int) DISTRIBUTED BY(a)'" against database "test_db_2"
#         And the user runs psql with "-c 'INSERT INTO test_schema_2.test_table_2 SELECT generate_series(1, 100)'" against database "test_db_2"
#        When the user runs "ggrebalance -x 1"
#        Then ggrebalance should return a return code of 1
#         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
#         And unset fault inject
#        When the user runs "ggrebalance -x 1"
#        Then ggrebalance should return a return code of 1
#         And ggrebalance should print "Can't start a new operation, because the previous one was interrupted" to logfile with latest timestamp
#        When the user runs "ggrebalance"
#        Then ggrebalance should return a return code of 0
#         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
#         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
#         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
#         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 1, row count = 100
#         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 1, row count = 100
#        When the user runs psql with "-c 'CREATE TABLE test_schema_1.test_table_3 (a int) DISTRIBUTED BY(a)'" against database "test_db_1"
#         And the user runs psql with "-c 'INSERT INTO test_schema_1.test_table_3 SELECT generate_series(1, 100)'" against database "test_db_1"
#        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 1, row count = 100

#    Examples:
#        | fault_name                                                                  |
#   NOT WORKING     | STATE_SETUP_SHRINK_SCHEMA_STARTED_begin                                     |
#   NOT WORKING     | STATE_SETUP_SHRINK_SCHEMA_STARTED_end                                       |
#        | on_enter_STATE_SETUP_SHRINK_SCHEMA_DONE_begin                               |
#        | on_enter_STATE_SETUP_SHRINK_SCHEMA_DONE_end                                 |
#        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_begin |
#        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_end   |
#        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_begin    |
#        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_end      |
#        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_begin                          |
#        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end                            |
#        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin                             |
#        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_end                               |
#        | on_enter_STATE_SHRINK_TABLES_STARTED_begin                                  |
#        | on_enter_STATE_SHRINK_TABLES_STARTED_end                                    |
#        | on_enter_STATE_SHRINK_TABLES_DONE_begin                                     |
#        | on_enter_STATE_SHRINK_TABLES_DONE_end                                       |
#        | on_enter_STATE_SHRINK_CATALOG_STARTED_begin                                 |
#        | on_enter_STATE_SHRINK_CATALOG_STARTED_end                                   |
#        | on_enter_STATE_SHRINK_CATALOG_DONE_begin                                    |
#        | on_enter_STATE_SHRINK_CATALOG_DONE_end                                      |

# TODO: replace distr check with the existing one
    Scenario Outline: ggrebalance - check rollback after interrupted state
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And set fault inject "<fault_name>"
         And database "test_db_1" exists
         And the user runs psql with "-c 'CREATE SCHEMA test_schema_1'" against database "test_db_1"
         And the user runs psql with "-c 'CREATE TABLE test_schema_1.test_table_1 (a int) DISTRIBUTED BY(a)'" against database "test_db_1"
         And the user runs psql with "-c 'INSERT INTO test_schema_1.test_table_1 SELECT generate_series(1, 100)'" against database "test_db_1"
         And the user runs psql with "-c 'CREATE TABLE test_schema_1.test_table_2 (a int) DISTRIBUTED BY(a)'" against database "test_db_1"
         And the user runs psql with "-c 'INSERT INTO test_schema_1.test_table_2 SELECT generate_series(1, 100)'" against database "test_db_1"
         And database "test_db_2" exists
         And the user runs psql with "-c 'CREATE SCHEMA test_schema_2'" against database "test_db_2"
         And the user runs psql with "-c 'CREATE TABLE test_schema_2.test_table_1 (a int) DISTRIBUTED BY(a)'" against database "test_db_2"
         And the user runs psql with "-c 'INSERT INTO test_schema_2.test_table_1 SELECT generate_series(1, 100)'" against database "test_db_2"
         And the user runs psql with "-c 'CREATE TABLE test_schema_2.test_table_2 (a int) DISTRIBUTED BY(a)'" against database "test_db_2"
         And the user runs psql with "-c 'INSERT INTO test_schema_2.test_table_2 SELECT generate_series(1, 100)'" against database "test_db_2"
        When the user runs "ggrebalance -x 1"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 2, row count = 100
        When the user runs psql with "-c 'CREATE TABLE test_schema_1.test_table_3 (a int) DISTRIBUTED BY(a)'" against database "test_db_1"
         And the user runs psql with "-c 'INSERT INTO test_schema_1.test_table_3 SELECT generate_series(1, 100)'" against database "test_db_1"
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 2, row count = 100

    Examples:
        | fault_name                                                                  |
#   NOT WORKING     | STATE_SETUP_SHRINK_SCHEMA_STARTED_begin                                     |NOT WORKING due to not existing at this moment dump of gparray
#   NOT WORKING     | STATE_SETUP_SHRINK_SCHEMA_STARTED_end                                       |NOT WORKING due to not existing at this moment dump of gparray
#        | on_enter_STATE_SETUP_SHRINK_SCHEMA_DONE_begin                               | - NOT WORKING due to not existing at this moment dump of gparray
#        | on_enter_STATE_SETUP_SHRINK_SCHEMA_DONE_end                                 | - NOT WORKING due to not existing at this moment dump of gparray
#        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_begin | - NOT WORKING due to not existing at this moment dump of gparray
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_end   |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_begin    |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_end      |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_begin                          |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end                            |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin                             |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_end                               |
        | on_enter_STATE_SHRINK_TABLES_STARTED_begin                                  |
        | on_enter_STATE_SHRINK_TABLES_STARTED_end                                    |
        | on_enter_STATE_SHRINK_TABLES_DONE_begin                                     |
        | on_enter_STATE_SHRINK_TABLES_DONE_end                                       |
#        | on_enter_STATE_SHRINK_CATALOG_STARTED_begin                                 |
#        | on_enter_STATE_SHRINK_CATALOG_STARTED_end                                   |
#        | on_enter_STATE_SHRINK_CATALOG_DONE_begin                                    |
#        | on_enter_STATE_SHRINK_CATALOG_DONE_end                                      |