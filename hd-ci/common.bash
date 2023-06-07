#!/usr/bin/env bash

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_PATH="${BASE_DIR}/.."
ROOT_PATH="${BASE_DIR}/../.."

BUILD_NUMBER=$(cat "${SRC_PATH}"/BUILD_NUMBER)
BUILD_TYPE=${BUILD_TYPE:-debug}

CENTOS_VERSION="$(rpm -E %{rhel} 2>/dev/null)"
OS_TYPE_EXT="el${CENTOS_VERSION}"
OS_ARCH="$(rpm -E %{_arch} 2>/dev/null)"
CBDB_VERSION=$("${SRC_PATH}"/getversion | cut -d ' ' -f1) 

source "${SRC_PATH}/concourse/scripts/common.bash"

# Copy code from concourse/scripts/setup_gpadmin_user.bash
determine_os() {
  local name version
  if [ -f /etc/redhat-release ]; then
    name="centos"
    version=$(sed </etc/redhat-release 's/.*release *//' | cut -f1 -d.)
  elif [ -f /etc/SuSE-release ]; then
    name="sles"
    version=$(awk -F " *= *" '$1 == "VERSION" { print $2 }' /etc/SuSE-release)
  elif grep -q ubuntu /etc/os-release ; then
    name="ubuntu"
    version=$(awk -F " *= *" '$1 == "VERSION_ID" { print $2 }' /etc/os-release | tr -d \")
  elif [ -f /etc/kylin-release ]; then
    name="kylin"
    version=$(sed -n </etc/os-release '/VERSION_ID=/s/.*="V\([0-9]\+\)"/\1/p')
  else
    echo "Could not determine operating system type" >/dev/stderr
    exit 1
  fi
  echo "${name}${version}"
}
OS_TYPE=$(determine_os)

setup_gpadmin_user() {
	"${SRC_PATH}"/concourse/scripts/setup_gpadmin_user.bash "$TEST_OS"
}

setup_cbdb() {
	if [ -z "${MAKE_TEST_COMMAND}" ]; then
		echo "FATAL: MAKE_TEST_COMMAND is not set"
		exit 1
	fi

	if [ -z "$TEST_OS" ]; then
		echo "FATAL: TEST_OS is not set"
		exit 1
	fi

	case "${TEST_OS}" in
	centos | ubuntu | sles) ;; #Valid
	*)
		echo "FATAL: TEST_OS is set to an invalid value: $TEST_OS"
		echo "Configure TEST_OS to be centos, or ubuntu"
		exit 1
		;;
	esac

	time install_and_configure_gpdb
	time setup_gpadmin_user
	time make_cluster

	if [ "${TEST_BINARY_SWAP}" == "true" ]; then
		time "${SRC_PATH}"/concourse/scripts/test_binary_swap_gpdb.bash
	fi
}

download_cbdb_tar_package() {
	fts_mode=$1
	source "${SRC_PATH}"/cbdb-artifacts.txt
	mkdir -p "${ROOT_PATH}"/bin_gpdb
	local download_url=${fts_mode:+$(if [ "${fts_mode}" == "external_fts" ]; then echo "${fts_mode}_"; fi)}TARGZ_cbdb_${OS_TYPE}_${OS_ARCH}_url
	
	
	curl -sSfL \
		-o "${ROOT_PATH}/bin_gpdb/bin_gpdb.tar.gz" \
		-z "${ROOT_PATH}/bin_gpdb/bin_gpdb.tar.gz" \
		"${!download_url}"
}

setup_env() {
	export MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=off' installcheck-world"
	export TEST_OS="centos"
	export TEST_BINARY_SWAP="false"
	export CONFIGURE_FLAGS="--enable-cassert --enable-tap-tests "
	export DUMP_DB="true"
	export PGUSER="gpadmin"
	export PGDATABASE="gpadmin"

	setup_cbdb
}
