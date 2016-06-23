#!/bin/bash


set -o pipefail
set -u -e -x

main() {
  local INPUT_RLS_DIR=bin_gpos_centos5_release
  local INPUT_DBG_DIR=bin_gpos_centos5_debug
  local INPUT_OSX_RLS_DIR=bin_gpos_osx32_release
  local INPUT_OSX_DBG_DIR=bin_gpos_osx32_debug
  local OUTPUT_DIR=gpos_github_release_stage
  env LC_ALL=C tar tf $INPUT_RLS_DIR/*.tar.gz | grep "libgpos.so." | sort -n | head -n 1 | sed 's/\.\/lib\/libgpos\.so\./v/' > $OUTPUT_DIR/tag.txt
  cp -v $INPUT_RLS_DIR/*.tar.gz $OUTPUT_DIR/
  cp -v $INPUT_DBG_DIR/*.tar.gz $OUTPUT_DIR/
  cp -v $INPUT_OSX_RLS_DIR/*.tar.gz $OUTPUT_DIR/
  cp -v $INPUT_OSX_DBG_DIR/*.tar.gz $OUTPUT_DIR/
  env GIT_DIR=gpos_src/.git git rev-parse HEAD > $OUTPUT_DIR/commit.txt
}

main "$@"
