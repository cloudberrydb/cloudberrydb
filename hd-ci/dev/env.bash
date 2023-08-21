#!/usr/bin/env bash

# internal usage: Currently, it is only used for rapid cleaning of the local environment and is not used in the pipeline
# Usage: source /code/gpdb_src/hd-ci/dev/env.bash
source /usr/local/cloudberry-db-devel/greenplum_path.sh
source /code/gpdb_src/gpAux/gpdemo/gpdemo-env.sh

if [[ -d "/code/dep/cmake-3.5.2-Linux-x86_64/bin" ]]; then
	export PATH=/code/dep/cmake-3.5.2-Linux-x86_64/bin:${PATH}
fi
