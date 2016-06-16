#!/bin/bash


set -o pipefail
set -u -e -x

main() {
  env LC_ALL=C tar tf bin_gpos_osx_release/bin_gpos_osx_release.tar.gz | grep "libgpos.dylib" | sort -n | head -n 1 | sed 's/\.\/lib\/libgpos\.dylib\./v/' > gpos_github_release_stage_osx/tag.txt
  cp -v bin_gpos_osx_release/bin_gpos_osx_release.tar.gz gpos_github_release_stage_osx/bin_gpos_osx_release.tar.gz
  cp -v bin_gpos_osx_debug/bin_gpos_osx_debug.tar.gz gpos_github_release_stage_osx/bin_gpos_osx_debug.tar.gz
  env GIT_DIR=gpos_src/.git git rev-parse HEAD > gpos_github_release_stage_osx/commit.txt
}

main "$@"
