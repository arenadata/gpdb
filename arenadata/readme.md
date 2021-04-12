
Behave tests now can run locally with docker-compose.

Feature files are located in `gpMgmt/test/behave/mgmt_utils`
Before run tests you need docker-image
```bash
docker build -t "hub.adsw.io/library/gpdb_regress:${BRANCH_NAME}" -f arenadata/Dockerfile .
```

Command to run features:
```bash
# Run all tests
bash arenadata/scripts/run_behave_tests.bash

# Run specific features
bash arenadata/scripts/run_behave_tests.bash gpstart gpstop
```

Tests will run with 4 parallels and store allure output files in `allure-results` folder
