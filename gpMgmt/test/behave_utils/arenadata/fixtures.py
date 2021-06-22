from behave import fixture


@fixture
def init_cluster(context, scenario):
    context.execute_steps(u"""
    Given the database is not running
        And a working directory of the test as '/tmp/{feature_name}'
        And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
        And a cluster is created with mirrors on "mdw" and "sdw1, sdw2, sdw3"
    """.format(feature_name=scenario.filename.split("/")[-1].split(".")[0]))
