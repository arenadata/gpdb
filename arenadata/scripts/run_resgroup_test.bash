#!/bin/bash

mkdir ssh_keys -p
if [ ! -e "ssh_keys/id_rsa" ]
then
  ssh-keygen -P "" -f ssh_keys/id_rsa
fi

docker network create resgroup_net

#start cdw and sdw1 containers
docker run -d --rm \
       	--name=resgroup-cdw -h cdw \
       	-e TEST_OS=centos \
       	--network=resgroup_net \
       	-v "$PWD/ssh_keys/id_rsa:/home/gpadmin/.ssh/id_rsa" \
       	-v "$PWD/ssh_keys/id_rsa.pub:/home/gpadmin/.ssh/id_rsa.pub" \
       	--privileged \
       	--sysctl kernel.sem="500 1024000 200 4096" \
       	$IMAGE \
       	bash -c 'set -e && sleep infinity'

docker run -d --rm \
        --name=resgroup-sdw1 -h sdw1 \
       	-e TEST_OS=centos \
       	--network=resgroup_net \
       	-v "$PWD/ssh_keys/id_rsa:/home/gpadmin/.ssh/id_rsa" \
       	-v "$PWD/ssh_keys/id_rsa.pub:/home/gpadmin/.ssh/id_rsa.pub" \
       	--privileged \
       	--sysctl kernel.sem="500 1024000 200 4096" \
       	$IMAGE bash -c 'set -e && sleep infinity'

#install gpdb and setup gpadmin user
docker exec resgroup-cdw bash -c "
	source gpdb_src/concourse/scripts/common.bash &&
	install_and_configure_gpdb &&
	gpdb_src/concourse/scripts/setup_gpadmin_user.bash" &

docker exec resgroup-sdw1 bash -c "
	source gpdb_src/concourse/scripts/common.bash &&
	install_and_configure_gpdb &&
	gpdb_src/concourse/scripts/setup_gpadmin_user.bash" &
wait

#add host key to known hosts
docker exec resgroup-cdw bash -c "ssh-keyscan cdw sdw1 >> /home/gpadmin/.ssh/known_hosts"
docker exec resgroup-sdw1 bash -c "ssh-keyscan cdw sdw1 >> /home/gpadmin/.ssh/known_hosts"

#grant access rights to group controllers
docker exec resgroup-cdw bash -c "chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset} &&
	mkdir /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
	chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
	chown -R gpadmin:gpadmin /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb"

docker exec resgroup-sdw1 bash -c "chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset} &&
        mkdir /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
        chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
        chown -R gpadmin:gpadmin /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb"

#create cluster
docker exec resgroup-cdw bash -c "source gpdb_src/concourse/scripts/common.bash && HOSTS_LIST='sdw1' make_cluster"

#run tests
docker exec -iu gpadmin resgroup-cdw bash -ex <<EOF
        source /usr/local/greenplum-db-devel/greenplum_path.sh
        source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
        export LDFLAGS="-L\${GPHOME}/lib"
        export CPPFLAGS="-I\${GPHOME}/include"
        export USER=gpadmin

        cd /home/gpadmin/gpdb_src
        ./configure --prefix=/usr/local/greenplum-db-devel \
            --without-zlib --without-rt --without-libcurl \
            --without-libedit-preferred --without-docdir --without-readline \
            --disable-gpcloud --disable-gpfdist --disable-orca \
            ${CONFIGURE_FLAGS}

        make -C /home/gpadmin/gpdb_src/src/test/regress
        ssh sdw1 mkdir -p /home/gpadmin/gpdb_src/src/test/regress </dev/null
        ssh sdw1 mkdir -p /home/gpadmin/gpdb_src/src/test/isolation2 </dev/null
        scp /home/gpadmin/gpdb_src/src/test/regress/regress.so \
            gpadmin@sdw1:/home/gpadmin/gpdb_src/src/test/regress/

        make PGOPTIONS="-c optimizer=off" installcheck-resgroup || (
            errcode=\$?
            find src/test/isolation2 -name regression.diffs \
            | while read diff; do
                cat <<EOF1

======================================================================
DIFF FILE: \$diff
----------------------------------------------------------------------

EOF1
                cat \$diff
              done
            exit \$errcode
        )
EOF

#clear
docker stop resgroup-cdw resgroup-sdw1
docker network rm resgroup_net

