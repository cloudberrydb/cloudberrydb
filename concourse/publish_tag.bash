#!/bin/bash


set -o pipefail
set -u -e -x

main() {
  env LC_ALL=C tar tf bin_gpos_centos5_release/bin_gpos_centos5_release.tar.gz | grep "libgpos.so." | sort -n | head -n 1 | sed 's/\.\/lib\/libgpos\.so\./v/' > gpos_github_release_stage/tag.txt
  cp -v bin_gpos_centos5_release/bin_gpos_centos5_release.tar.gz gpos_github_release_stage/bin_gpos_centos5_release.tar.gz
  cp -v bin_gpos_centos5_debug/bin_gpos_centos5_debug.tar.gz gpos_github_release_stage/bin_gpos_centos5_debug.tar.gz
  env GIT_DIR=gpos_src/.git git rev-parse HEAD > gpos_github_release_stage/commit.txt
}

main "$@"
