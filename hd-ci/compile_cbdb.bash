#!/usr/bin/env bash
set -exo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source /etc/profile
source "${BASE_DIR}"/common.bash

CBDB_VERSION=$(${SRC_PATH}/getversion --short)



function download_dependencies() {
	download_jansson /usr/lib
	\cp /usr/lib/libjansson.so* /lib64/
}

function compile_cbdb() {
	fts_mode=$1
	export GPDB_SRC_PATH="${SRC_PATH}"
	export CONFIGURE_FLAGS="--enable-cassert --enable-tap-tests --enable-debug-extensions --with-openssl"
	if [[ ${BUILD_TYPE} == "release" ]]; then
		export CPPFLAGS="${CXXFLAGS} -Wno-unused-function -Wno-unused-variable"
		export CONFIGURE_FLAGS="--disable-cassert --disable-tap-tests --disable-debug-extensions --with-openssl"
	fi

	if [ "${fts_mode}" = "external_fts" ]; then
		export CONFIGURE_FLAGS="${CONFIGURE_FLAGS} --enable-external-fts"
	fi
	"${GPDB_SRC_PATH}"/concourse/scripts/compile_gpdb.bash
}

function upload_cbdb_tar_and_rpm_package() {
	fts_mode=$1
	generate_docker_tag=$2
	tar_package_path=$(ls "${ROOT_PATH}"/cloudberrydb/gpdb_artifacts/server-build*)

	cp $tar_package_path /tmp

	rpm_package_path=$(ls ~/rpmbuild/RPMS/"${OS_ARCH}"/cloudberry-db*.rpm)

	cp $rpm_package_path /tmp
	if [[ ${fts_mode} == "internal_fts" ]]; then
		cat <<EOF >>"${SRC_PATH}/cbdb-artifacts.txt"
os_type=${OS_TYPE_EXT}
os_arch=${OS_ARCH}
cbdb_major_version=1.x
cbdb_version=${CBDB_VERSION}
build_number=${BUILD_NUMBER}
build_type=${BUILD_TYPE}
internal_tar_download_url=${tar_package_path}
internal_rpm_download_url=${rpm_package_path}
CBDB_VERSION=${CBDB_VERSION}
EOF
	else
		cat <<EOF >>"${SRC_PATH}/cbdb-artifacts.txt"
external_fts_RELEASE_cbdb_${OS_TYPE}_${OS_ARCH}_url=${rpm_download_url}
external_fts_TARGZ_cbdb_${OS_TYPE}_${OS_ARCH}_url=${tar_download_url}
EOF
	fi

	if [ "${generate_docker_tag}" = "true" ]; then
		release_image_k8s ${rpm_package_path} ${rpm_upload_file_name}
		echo ${rpm_package_path} ${rpm_upload_file_name}
	fi

	cat "${SRC_PATH}/cbdb-artifacts.txt"
}

function package_create_rpm() {
	mkdir -p ~/rpmbuild/SOURCES/
	cp -a "${ROOT_PATH}/cloudberrydb/gpdb_artifacts/bin_gpdb.tar.gz" ~/rpmbuild/SOURCES/bin_cbdb.tar.gz

	version=$(echo "${CBDB_VERSION}" | tr '-' '_')
	PATH=/usr/bin:$PATH rpmbuild \
		--define="version ${version}" \
		--define="cbdb_version ${CBDB_VERSION}" \
		-bb "${SRC_PATH}/.cloudberry-db.spec"
}

function release_image_k8s() {
	rpm_package_path=$1
	rpm_upload_file_name=$2
	pushd ${SRC_PATH}/deploy/k8s
	echo "{ \"insecure-registries\": [\"${DOCKER_REPO}\"] }" >> /etc/docker/daemon.json
	echo "push new docker image to ${DOCKER_REPO}"
	systemctl daemon-reload
	systemctl restart docker
	tag_name=$(basename "${rpm_package_path}" ".rpm")-"${BUILD_NUMBER}"-"${BUILD_TYPE}"
	tag_name=$(echo "${tag_name}" | sed 's/\+/-/g')
	docker build . -t ${DOCKER_REPO}/docker/cbdb:${tag_name} --build-arg PACKAGE=${rpm_upload_file_name} --build-arg BUILD_TYPE=${BUILD_TYPE}
	echo ${ARTIFACTORY_PASSWORD} | docker login --username ${ARTIFACTORY_USERNAME} --password-stdin ${DOCKER_REPO}
	docker push ${DOCKER_REPO}/docker/cbdb:${tag_name}
	popd
}

function release_rpm_internal_fts() {
	compile_cbdb "internal_fts"
	package_create_rpm 
	upload_cbdb_tar_and_rpm_package "internal_fts" ""
}

function release_rpm_external_fts() {
	gen_docker_tag=$1
	compile_cbdb "external_fts"
	package_create_rpm 
	upload_cbdb_tar_and_rpm_package "external_fts" ${gen_docker_tag}
}

main() {
	download_dependencies

	pushd ${SRC_PATH}
	gpdb_dir_name=$(basename "$(pwd)")
	cp -r ${ROOT_PATH}/${gpdb_dir_name} ${ROOT_PATH}/gpdb_src_tmp
	popd

	release_rpm_internal_fts
	# release_rpm_external_fts $1
	pushd ${ROOT_PATH}
	cp ${gpdb_dir_name}/cbdb-artifacts.txt .
	rm -rf ${gpdb_dir_name} ~/rpmbuild ${OUTPUT_ARTIFACT_DIR:=gpdb_artifacts} 
	mv gpdb_src_tmp ${gpdb_dir_name}
	mv cbdb-artifacts.txt ${gpdb_dir_name}
	popd


}

main $1
