#!/bin/bash -l

set -euxo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

substitute_GP_VERSION() {
  GP_VERSION=$("$DIR/../../getversion" --short)
  GPDB_SRC_TAR_GZ=${GPDB_SRC_TAR_GZ//@GP_VERSION@/${GP_VERSION}}
}

echo_expected_env_variables() {
  echo "Source code tarball file to be created: ${GPDB_SRC_TAR_GZ}"
}

_main() {
  substitute_GP_VERSION
  echo_expected_env_variables
  cd gpdb_src

  tar -czf "../${GPDB_SRC_TAR_GZ}" gpMgmt/test gpMgmt/bin/pythonSrc gpMgmt/bin/Makefile gpMgmt/Makefile.behave
}

_main
