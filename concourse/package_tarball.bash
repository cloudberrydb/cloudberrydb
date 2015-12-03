#!/bin/bash

set -u -e -x

main() {
  local build_task_name
  local output_name
  build_task_name="$1"
  output_name="$2"
  cd "$1"
  pushd install
  tar czf "../../${output_name}" .
}

main "$@"
