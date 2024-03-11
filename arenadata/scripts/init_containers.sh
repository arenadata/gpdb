#!/bin/bash
set -eo pipefail

project="$1"

mkdir ssh_keys -p
if [ ! -e "ssh_keys/id_rsa" ]
then
  ssh-keygen -P "" -f ssh_keys/id_rsa
fi

shift

#up services specified in argument or all if service list is not specified
docker-compose -p $project -f arenadata/docker-compose.yaml --env-file arenadata/.env up -d $@


if [[ $# -eq 0 ]]; then
  services=$(docker-compose -p $project -f arenadata/docker-compose.yaml config --services | tr '\n' ' ')
else
  services="$@"
fi
# Prepare ALL containers first
for service in $services
do
  docker-compose -p $project -f arenadata/docker-compose.yaml exec -T \
    $service bash -c "mkdir -p /data/gpdata && chmod -R 777 /data &&
      source gpdb_src/concourse/scripts/common.bash && install_gpdb &&
      ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash" &
done
wait

# Add host keys to known_hosts after containers setup
for service in $services
do
  docker-compose -p $project -f arenadata/docker-compose.yaml exec -T \
    $service bash -c "ssh-keyscan ${services/$service/} >> /home/gpadmin/.ssh/known_hosts" &
done
wait
