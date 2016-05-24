#!/bin/bash


set -o pipefail
set -u -e -x

main() {
  env LC_ALL=C tar tf bin_orca_centos5_release/bin_orca_centos5_release.tar.gz | grep "libgpopt.so." | sort -n | head -n 1 | sed 's/\.\/lib\/libgpopt\.so\./v/' > orca_github_release_stage/tag.txt
  cp -v bin_orca_centos5_release/bin_orca_centos5_release.tar.gz orca_github_release_stage/bin_orca_centos5_release.tar.gz
  cp -v bin_orca_centos5_debug/bin_orca_centos5_debug.tar.gz orca_github_release_stage/bin_orca_centos5_debug.tar.gz
  env GIT_DIR=orca_src/.git git rev-parse HEAD > orca_github_release_stage/commit.txt
}

main "$@"
