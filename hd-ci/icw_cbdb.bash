#!/usr/bin/env bash
set -exo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${BASE_DIR}"/common.bash

mkdir -p "${ROOT_PATH}/sqldump"

compile_jansson() {
        wget https://artifactory.hashdata.xyz/artifactory/utility/jansson-2.13.1.tar.gz && \
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
	ETCD_DOWNLOAD_URL=https://artifactory.hashdata.xyz/artifactory/utility/${ETCD_FILE_NAME}.tar.gz
	wget ${ETCD_DOWNLOAD_URL} -O /tmp/${ETCD_FILE_NAME}.tar.gz
	tar -xvf /tmp/${ETCD_FILE_NAME}.tar.gz -C /tmp
	cp /tmp/${ETCD_FILE_NAME}/etcd ${GREENPLUM_INSTALL_DIR}/bin
	cp /tmp/${ETCD_FILE_NAME}/etcdctl ${GREENPLUM_INSTALL_DIR}/bin
	rm -rf /tmp/${ETCD_FILE_NAME} /tmp/${ETCD_FILE_NAME}.tar.gz
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
	local rpm_download_url=${fts_mode:+$(if [ "${fts_mode}" == "external_fts" ]; then echo "${fts_mode}_"; fi)}RELEASE_cbdb_${OS_TYPE}_${OS_ARCH}_url
	[ -z "$rpm_download_url" ] && echo "rpm download url is not set" >&2 && exit 1
	curl -O "${!rpm_download_url}"
	yum install -y c*.rpm
	cd "$INSTALL_DIR"
	tar czf "${ROOT_PATH}/bin_gpdb_rpm/bin_gpdb.tar.gz" ./*
	popd
	cp -f "${ROOT_PATH}/bin_gpdb_rpm/bin_gpdb.tar.gz" "${ROOT_PATH}/bin_gpdb/"
}
main() {
	fts_mode=$1
	download_cbdb_tar_package ${fts_mode}
	if [ "$BUILD_TYPE" == "release" ]; then
		prepare_release $1
	fi
	compile_jansson
	download_etcd
	icw_cbdb ${fts_mode}
}

main $1
