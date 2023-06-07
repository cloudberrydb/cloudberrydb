#!/usr/bin/env bash
set -exo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${BASE_DIR}/../common.bash"

# internal usage: 目前只是用于本地环境的快速搭建, 没有用在pipeline中
# Usage: /code/gpdb_src/hd-ci/dev/prepare-env.bash
download_demo_cbdb_tar_gz() {
	if [[ ! -f "${ROOT_PATH}/bin_gpdb/bin_gpdb.tar.gz" ]]; then
		mkdir -p "${ROOT_PATH}/bin_gpdb/"
		wget --output-document "${ROOT_PATH}/bin_gpdb/bin_gpdb.tar.gz" \
			http://artifactory.hashdata.xyz/artifactory/hashdata-repository/centos7/x86_64/cbdb/debug/server-build-8.0.1-alpha.0+dev.122.g303e7a7041-rhel7_x86_64-20753-debug.tar.gz
	fi
}

main() {
	download_demo_cbdb_tar_gz
	setup_env
}

main
