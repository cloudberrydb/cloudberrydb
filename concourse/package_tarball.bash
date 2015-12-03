#!/bin/bash

set -u -e

main() {
  local output_name
  output_name="$1"
  cd build_and_test_centos5_debug
  pushd install
  tar czf "../../${output_name}" .
}

main "$@"
