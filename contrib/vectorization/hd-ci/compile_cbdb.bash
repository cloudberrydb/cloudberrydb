#!/usr/bin/env bash
set -exo pipefail

source ${CBDB_PATH}/hd-ci/common.bash

CBDB_VERSION=$(${CBDB_PATH}/getversion --short)

compile_cbdb() {
	export GPDB_SRC_PATH="${CBDB_PATH}"
	export CONFIGURE_FLAGS="--enable-cassert --enable-tap-tests --enable-debug-extensions"
	if [[ ${BUILD_TYPE} == "release" ]]; then
		export CPPFLAGS="${CXXFLAGS} -Wno-unused-function -Wno-unused-variable"
		export CONFIGURE_FLAGS="--disable-cassert --disable-tap-tests --disable-debug-extensions "
	fi

	${CI_PROJECT_DIR}/hd-ci/compile_gpdb.bash
}

function upload_cbdb_tar_and_rpm_package() {
	tar_package_path=$(ls "${ROOT_PATH}"/gpdb_artifacts/server-build*)
	tar_upload_file_name=$(basename "${tar_package_path}" ".tar.gz")-"${BUILD_NUMBER}"-"${BUILD_TYPE}".tar.gz
	tar_download_url="$BUCKET_INTERMEDIATE/${OS_TYPE}/${OS_ARCH}/cbdb/${BUILD_TYPE}/${tar_upload_file_name}"
	tar_download_url_latest="$BUCKET_INTERMEDIATE/${OS_TYPE}/${OS_ARCH}/cbdb/${BUILD_TYPE}/vec_cbdb_latest.tar.gz"

	# upload tar.gz file
	curl -s -S -f -u "${ARTIFACTORY_USERNAME}":"${ARTIFACTORY_PASSWORD}" \
		-X PUT "${tar_download_url}" \
		-T "${tar_package_path}"
	curl -s -S -f -u "${ARTIFACTORY_USERNAME}":"${ARTIFACTORY_PASSWORD}" \
		-X PUT "${tar_download_url_latest}" \
		-T "${tar_package_path}"
	cat <<EOF >>"${CBDB_PATH}/cbdb-artifacts.txt"
os_type=${OS_TYPE_EXT}
os_arch=${OS_ARCH}
cbdb_major_version=1.x
cbdb_version=${CBDB_VERSION}
build_number=${BUILD_NUMBER}
build_type=${BUILD_TYPE}
internal_tar_download_url=${tar_download_url}
TARGZ_cbdb_${OS_TYPE}_${OS_ARCH}_url=${tar_download_url}
CBDB_VERSION=${CBDB_VERSION}
EOF

	cat "${CBDB_PATH}/cbdb-artifacts.txt"
}

main() {
	compile_cbdb
	upload_cbdb_tar_and_rpm_package
}

main
