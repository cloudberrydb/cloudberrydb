#!/usr/bin/env bash
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${BASE_DIR}/../common.bash"

# internal usage: 目前只是用于本地环境的快速清理, 没有用在pipeline中
# Usage: /code/gpdb_src/hd-ci/dev/clean-env.bash
main() {
	killall postgres
	rm -rf /usr/local/greenplum-db*
	groupdel supergroup
	echo 20130519 > "${SRC_PATH}"/BUILD_NUMBER
	echo done
}

main
