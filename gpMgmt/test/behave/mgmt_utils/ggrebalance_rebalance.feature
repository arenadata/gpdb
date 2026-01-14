@ggrebalance_rebalance
Feature: ggrebalance behave tests (rebalance scenarios)

    Scenario Outline: test 1.1. rebalance - check scenario, when we remove/add a host and rebalance the cluster (with different batch size).
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance -B <batch_size> -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'p'"
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'm'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'p'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When the user runs "ggrebalance -x 6"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Cluster is already balanced, no segment moves will be held." to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -B <batch_size> -x 6 --add-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p'"
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'm'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When the user runs "ggrebalance -x 6"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Cluster is already balanced, no segment moves will be held." to logfile with latest timestamp

    Examples:
        | batch_size |
        | 1          |
        | 16         |
        | 64         |
        | 128        |

