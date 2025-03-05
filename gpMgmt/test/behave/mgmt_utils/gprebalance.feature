@gprebalance
Feature: Tests for gprebalance
    Scenario: gprebalance balances cluster after expansion on fixed hosts
        Given the database is not running
        And a working directory of the test as '/data/gpdata/gpexpand'
        And a temporary directory under "/data/gpdata/gpexpand/expandedData" to expand into
        And a cluster is created with mirrors on "cdw" and "sdw1,sdw2"
        And the cluster is setup for an expansion on hosts "cdw,sdw1,sdw2"
        And the number of segments have been saved
        When the user runs gpexpand with segment imbalance for a two-node cluster with mirrors
        Then verify that the cluster has 4 new segments
        When the user runs "gprebalance -s"
        Then gprebalance should return a return code of 0
        When the user runs "gprebalance -s"
        Then gprebalance should print "[INFO]:-Cluster is already balanced" escaped to stdout
