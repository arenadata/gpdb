#!/usr/bin/env bash
set -x -o pipefail

behave_tests_dir="gpMgmt/test/behave/mgmt_utils"

if [ $# -eq 0 ]
then
  features=`ls $behave_tests_dir -1 | grep feature | sed 's/\.feature$//'`
else
  for feature in $@
  do
    if [ ! -f "$behave_tests_dir/$feature.feature" ]
    then
      echo "Feature '$feature' doesn't exists"
      exit 1
    fi
  done
  features=$@
fi

processes=3

rm -rf allure-results
mkdir allure-results -pm 777
mkdir ssh_keys -p
if [ ! -e "ssh_keys/id_rsa" ]
then
  ssh-keygen -P "" -f ssh_keys/id_rsa
fi

run_feature() {
  local feature=$1
  echo "Started $feature behave tests"
  docker-compose -p $feature -f arenadata/docker-compose.yaml --env-file arenadata/.env up -d
  docker-compose -p $feature -f arenadata/docker-compose.yaml exec -T \
    -e BEHAVE_FLAGS="--tags $feature \
      -f behave_utils.arenadata.formatter:CustomFormatter \
      -o non-existed-output \
      -f allure_behave.formatter:AllureFormatter \
      -o /tmp/allure-results"  \
    mdw gpdb_src/arenadata/scripts/behave_gpdb.bash
  docker-compose -p $feature -f arenadata/docker-compose.yaml --env-file arenadata/.env down -v
}

for feature in $features
do
   run_feature $feature &

   if [[ $(jobs -r -p | wc -l) -ge $processes ]]; then
      wait -n
   fi
done
wait
