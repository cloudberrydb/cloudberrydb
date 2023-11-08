#!/bin/bash
user=${1}

if test -z "$user"
then
    user="gpadmin"
fi

docker_image_version=cloudberrydb/cloudberrydb:devel-devtoolset-10-cbdb-docker-ubuntu-2019-20230313

docker pull ${docker_image_version}

mount_path=$(cd "$(dirname "$0")/../..";pwd)

cbdb_mount_opt=""

if test -n "${CBDB_TEST_REPO_PATH}"
then
    cbdb_test_mount_opt=" -v ${CBDB_TEST_REPO_PATH}:/home/${user}/workspace/cbdb_testrepo:rw "
fi

if test -n "${CBDB_TEST_TPCH_PATH}"
then
    cbdb_tpch_mount_opt=" -v ${CBDB_TEST_TPCH_PATH}:/home/${user}/workspace/tpch:rw "
fi

docker run --privileged -v ${mount_path}:/home/${user}/workspace/cbdb:ro ${cbdb_test_mount_opt} ${cbdb_tpch_mount_opt} -u root -it ${docker_image_version} /bin/bash


