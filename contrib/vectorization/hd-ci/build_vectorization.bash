#!/usr/bin/env bash
set -exo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CBDB_PATH}/hd-ci/common.bash"

install_tpch_data() {
	mkdir -p ${GIT_CLONE_PATH}/data/tpch
	wget https://artifactory.hashdata.xyz/artifactory/list/utility/tpch_data.tar
	tar xf tpch_data.tar -C ${GIT_CLONE_PATH}/data/tpch
	rm -f tpch_data.tar
}

build_test_vectorization() {
	(
		chown -R gpadmin:gpadmin "${GIT_CLONE_PATH}"
		chown -R gpadmin:gpadmin ${INSTALL_DIR}/
		source ${INSTALL_DIR}/greenplum_path.sh 
		export PYTHONPATH="/opt/rh/llvm-toolset-13.0/root/usr/lib/python3.6/site-packages"
		export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:${INSTALL_DIR}/lib/pkgconfig
		ldconfig
		
		rm -rf /tmp/conanfile.txt

		su - gpadmin -c bash -- -e <<EOF
		set -exo pipefail
		source ${INSTALL_DIR}/greenplum_path.sh 
		source ${ROOT_PATH}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
		pushd "${GIT_CLONE_PATH}"
		./dependencies.sh
		make install
		make load
		echo "optimizer is ${MAKEOPTS}"
        ${MAKEOPTS} make ${TEST_TYPE}
		if [[ ${TEST_TYPE} == "vec_test" ]]; then
			if [ -f regression.diffs ]
			then
				cat regression.diffs 
			fi
		elif [[ ${TEST_TYPE} == "icw_aocs" ]]; then
			if [ -f src/test/regress/regression.diffs ]
			then
				cat src/test/regress/regression.diffs
			fi
		else
			if [ -f src/test/regress/regression.diffs ]
			then
				cat src/test/regress/regression.diffs 
			fi
		fi 
		popd
EOF
	)
}

download_cbdb() {
	mkdir -p "${ROOT_PATH}"/bin_gpdb
	local download_url_var=TARGZ_cbdb_${OS_TYPE}_${OS_ARCH}_url
	local download_url="${!download_url_var}"
	if [ -z "${download_url}" ]; then
		download_url="$BUCKET_INTERMEDIATE/${OS_TYPE}/${OS_ARCH}/cbdb/${BUILD_TYPE}/vec_cbdb_latest.tar.gz"
	fi
	curl -sSfL \
		-o "${ROOT_PATH}/bin_gpdb/bin_gpdb.tar.gz" \
		-z "${ROOT_PATH}/bin_gpdb/bin_gpdb.tar.gz" \
		"${download_url}"
}

main() {
	download_cbdb
	setup_env
	install_tpch_data
	build_test_vectorization
}

main
