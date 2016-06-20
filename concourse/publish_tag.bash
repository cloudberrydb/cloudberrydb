#!/bin/bash


set -o pipefail
set -u -e -x

main() {
  env LC_ALL=C tar tf bin_orca_release/*.tar.gz | grep "libgpopt.so." | sort -n | head -n 1 | sed 's/\.\/lib\/libgpopt\.so\./v/' > orca_github_release_stage/tag.txt
  cp -v bin_orca_release/*.tar.gz orca_github_release_stage/
  env GIT_DIR=orca_src/.git git rev-parse HEAD > orca_github_release_stage/commit.txt
}

main "$@"
