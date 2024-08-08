#!/usr/bin/env bash
set -exo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${BASE_DIR}"/common.bash

mkdir -p "${ROOT_PATH}/sqldump"

compile_jansson() {
        # wget https://artifactory.hashdata.xyz/artifactory/utility/jansson-2.13.1.tar.gz && \
        cp /opt/jansson-2.13.1.tar.gz . && \
        tar -xvf jansson-2.13.1.tar.gz && \
        rm -rf jansson-2.13.1.tar.gz && \
        pushd .
        cd jansson-2.13.1 && \
        ./configure --prefix=/usr --disable-static && \
        make && \
        make install && \
        cp -f /usr/lib/libjansson.so* /usr/lib64/ && \
        popd
}

function download_etcd() {
	arch=`uname -i`
	if [[ $arch = x86_64 ]]
	then
		ETCD_FILE_NAME=etcd-v3.3.25-linux-amd64 
	else
		ETCD_FILE_NAME=etcd-v3.3.25-linux-arm64
	fi

	tar -xvf /opt/${ETCD_FILE_NAME}.tar.gz -C /opt
	cp /opt/${ETCD_FILE_NAME}/etcd ${GREENPLUM_INSTALL_DIR}/bin
	cp /opt/${ETCD_FILE_NAME}/etcdctl ${GREENPLUM_INSTALL_DIR}/bin
	export ETCD_UNSUPPORTED_ARCH=arm64
}

icw_cbdb() {
	fts_mode=$1
	# setup ENV before running this script: MAKE_TEST_COMMAND, TEST_OS, TEST_BINARY_SWAP, DUMP_DB
	export CONFIGURE_FLAGS="--disable-cassert --enable-tap-tests --enable-debug-extensions --with-openssl"
	if [ "${fts_mode}" = "external_fts" ]; then
		export CONFIGURE_FLAGS="${CONFIGURE_FLAGS} --enable-external-fts"
	fi
	
	"${SRC_PATH}"/concourse/scripts/ic_gpdb.bash
	# find bad GPOS_ASSERT failure in log file
	find "${SRC_PATH}"/gpAux/gpdemo/datadirs/ -type d -name log -exec grep -rn debug_gpos_assert 2>/dev/null {} \; 
}

prepare_release() {
	# 1. Download cbdb rpm package
	# 2. Install cbdb rpm package
	# 3. Package the installed cbdb files and replace the bin_gpdb.tar.gz in bin_gpdb/

	fts_mode=$1
	[ -z "$INSTALL_DIR" ] && echo "INSTALL_DIR is not set" >&2 && exit 1
	echo "Install cbdb rpm package and replace the tar.gz"

	pushd "${ROOT_PATH}"
	mkdir -p bin_gpdb_rpm && cd bin_gpdb_rpm
	yum install -y /opt/c*.rpm
	cd "$INSTALL_DIR"
	tar czf "${ROOT_PATH}/bin_gpdb_rpm/bin_gpdb.tar.gz" ./*
	popd
	cp -f "${ROOT_PATH}/bin_gpdb_rpm/bin_gpdb.tar.gz" "${ROOT_PATH}/bin_gpdb/"
}

prepare_resgroup() {
	# download the cgroup tools and setup the test dir
	yum -y install libcgroup libcgroup-tools
	cgcreate -a gpadmin:gpadmin -g cpu:gpdb
	cgcreate -a gpadmin:gpadmin -g cpuacct:gpdb
	cgcreate -a gpadmin:gpadmin -g cpuset:gpdb
}


main() {
	fts_mode=$1
	download_cbdb_tar_package ${fts_mode}
	if [ "$BUILD_TYPE" == "release" ]; then
		prepare_release $1
	fi
	prepare_resgroup
	compile_jansson
	download_etcd
	icw_cbdb ${fts_mode}
}

main $1
