#!/usr/bin/env bash
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${BASE_DIR}/../common.bash"

# internal usage: Currently, it is only used for rapid cleaning of the local environment and is not used in the pipeline
# Usage: /code/gpdb_src/hd-ci/dev/clean-env.bash
main() {
	killall postgres
	rm -rf /usr/local/greenplum-db*
	groupdel supergroup
	echo 20130519 > "${SRC_PATH}"/BUILD_NUMBER
	echo done
}

main
