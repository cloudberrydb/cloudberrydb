#!/bin/bash

set -euxo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

substitute_GP_VERSION() {
  GP_VERSION=$("$DIR/../../getversion" --short)
  GPDB_SRC_TAR_GZ=${GPDB_SRC_TAR_GZ//@GP_VERSION@/${GP_VERSION}}
}

echo_expected_env_variables() {
  echo "Source code tarball file to be created: $GPDB_SRC_TAR_GZ"
}

_main() {
  substitute_GP_VERSION
  echo_expected_env_variables
  cd gpdb_src

  #Only the TINC directory will be needed downstream at the moment, so
  #we will tar it up if it exists. If not, we will just use an empty
  #file for our packaged source code
  if [ -d "src/test/tinc" ]; then
      tar -czf ../$GPDB_SRC_TAR_GZ src/test/tinc
  else
      touch ../$GPDB_SRC_TAR_GZ
  fi
}

_main "$@"
