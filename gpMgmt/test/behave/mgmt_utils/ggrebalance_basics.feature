@ggrebalance_basics
Feature: ggrebalance behave tests

    Scenario Outline: test 1. validate incompatible option combinations
        Given a standard local demo cluster is running
        When the user runs "ggrebalance <options>"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "<error_message>" to stdout
        Given <run_status>
        
        Examples: Mutually exclusive options
          | options                                                     | error_message                                                         | run_status |
          | --target-hosts sdw1,sdw2 --target-hosts-file /tmp/hosts.txt | Can't use together options '--target-hosts' and '--target-hosts-file' |  stub |
          | --target-hosts sdw1,sdw2 --add-hosts sdw3                   | Can't use together options '--target-hosts' and '--add-hosts'         | stub       |
          | --target-hosts sdw1,sdw2 --remove-hosts sdw3                | Can't use together options '--target-hosts' and '--remove-hosts'      | stub       |
          | --add-hosts sdw3 --add-hosts-file /tmp/add.txt              | Can't use together options '--add-hosts' and '--add-hosts-file'       | stub       |
          | --remove-hosts sdw3 --remove-hosts-file /tmp/rm.txt         | Can't use together options '--remove-hosts' and '--remove-hosts-file' | stub       |
          | --add-hosts sdw3 --remove-hosts sdw3                        | Can't use together options '--add-hosts' and '--remove-hosts'         | stub       |
          | --target-datadirs '/data/p/gpseg{content},/data/m/gpseg{content}' --target-datadirs-file /tmp/datadirs.txt | Can't use together options '--target-datadirs' and '--target-datadirs-file' | stub |
          | --mirror-mode grouped --skip-rebalance                      | Can't use together options '--skip-rebalance' and '--mirror-mode'     | stub       |
          | -m spread --skip-rebalance                                  | Can't use together options '--skip-rebalance' and '--mirror-mode'     | stub       |
          | -c --target-hosts sdw1,sdw2                                 | Can't use together options '--clean-required' and '--target-hosts'    | stub       |
          | -c --add-hosts sdw3                                         | Can't use together options '--clean-required' and '--add-hosts'       | stub       |
          | -c --target-datadirs '/data/p/gpseg{content},/data/m/gpseg{content}' | Can't use together options '--clean-required' and '--target-datadirs'   | stub    |
          | -c -x 2                                                     | Can't use together options '--clean-required' and '--target-segment-count' | stub    |
          | -c --mirror-mode grouped                                    | Can't use together options '--clean-required' and '--mirror-mode'       | stub     |
          | -c --skip-rebalance                                         | Can't use together options '--clean-required' and '--skip-rebalance'    | stub     |
          | -c --show-plan                                              | Can't use together options '--clean-required' and '--show-plan'         | stub     |
          | -c --analyze                                                | Can't use together options '--clean-required' and '--analyze'           | stub     |
          | -c --replay-lag 1                                           | Can't use together options '--clean-required' and '--replay-lag'        | stub     |
          | -c --skip-resource-estimation                               | Can't use together options '--clean-required' and '--skip-resource-estimation' |  the database is not running |

    Scenario: test 2. ggrebalance simple scenarios
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2"
         And segment information for content 1 is saved in context
         And segment information for content 2 is saved in context
         And segment information for content 3 is saved in context
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 4 --skip-rebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Skipping rebalance" to logfile with latest timestamp
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Cleanup is not required." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -x 2 --skip-rebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And verify no segment running for saved segment information
        When the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Previous run was completed successfully. Please execute cleanup before a new run." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Previous run was completed successfully. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Cleanup is complete" to logfile with latest timestamp

    Scenario: test 3. check cleanup after the target segment count was updated
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_end"
        When the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance -c -y"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Reset numsegments to default is done." to logfile with latest timestamp
         And ggrebalance should print "Cleanup is complete" to logfile with latest timestamp

    Scenario: test TBD-1. rebalance - check log directory option. TBD - move to 'options' test suite
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -l "/tmp/ggrebalance_logs" -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And gpAdminLogs directory has no "ggrebalance*" files
         And gpAdminLogs directory has no "gpmovemirrors*" files
         And gpAdminLogs directory has no "gprecoverseg*" files
         And "/tmp/ggrebalance_logs" directory has "ggrebalance*" files
         And "/tmp/ggrebalance_logs" directory has "gpmovemirrors*" files
         And "/tmp/ggrebalance_logs" directory has "gprecoverseg*" files
         And all files in "/tmp/ggrebalance_logs" directory are deleted
        When the user runs "ggrebalance -c"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -l "/tmp/ggrebalance logs" -x 6 --add-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And gpAdminLogs directory has no "ggrebalance*" files
         And gpAdminLogs directory has no "gpmovemirrors*" files
         And gpAdminLogs directory has no "gprecoverseg*" files
         And "/tmp/ggrebalance logs" directory has "ggrebalance*" files
         And "/tmp/ggrebalance logs" directory has "gpmovemirrors*" files
         And "/tmp/ggrebalance logs" directory has "gprecoverseg*" files
         And all files in "/tmp/ggrebalance logs" directory are deleted

    Scenario: test TBD-2. rebalance - check '--analyze' option. TBD - move to 'options' test suite
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS not_analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  not_analyzed_tables_cnt  |
          |  2                        |
        When the user runs "ggrebalance --analyze -x 3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NOT NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  analyzed_tables_cnt  |
          |  2                    |

    Scenario: test TBD-3. Check that '--replay-lag' option takes effect.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --replay-lag 0 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And gprecoverseg should print "0 bytes of wal is still to be replayed on mirror with dbid.*, let mirror catchup on replay then trigger rebalance" regex to logfile
         And all files in gpAdminLogs directory are deleted
