#!/usr/bin/env bash

# internal usage: 目前只是用于本地环境快速设置cbdb环境变量, 没有用在pipeline中
# Usage: source /code/gpdb_src/hd-ci/dev/env.bash
source /usr/local/cloudberry-db-devel/greenplum_path.sh
source /code/gpdb_src/gpAux/gpdemo/gpdemo-env.sh

if [[ -d "/code/dep/cmake-3.5.2-Linux-x86_64/bin" ]]; then
	export PATH=/code/dep/cmake-3.5.2-Linux-x86_64/bin:${PATH}
fi
