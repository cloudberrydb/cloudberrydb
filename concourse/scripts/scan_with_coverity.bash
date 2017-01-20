#!/bin/bash -l
set -exo pipefail

GREENPLUM_INSTALL_DIR=/usr/local/greenplum-db-devel
export GPDB_ARTIFACTS_DIR
GPDB_ARTIFACTS_DIR=$(pwd)/$OUTPUT_ARTIFACT_DIR

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function prep_env_for_centos() {
  BLDARCH=rhel7_x86_64
  echo "Detecting java7 path ..."
  java7_packages=$(rpm -qa | grep -F java-1.7)
  java7_bin="$(rpm -ql $java7_packages | grep /jre/bin/java$)"
  alternatives --set java "$java7_bin"
  export JAVA_HOME="${java7_bin/jre\/bin\/java/}"
  ln -sf /usr/bin/xsubpp /usr/share/perl5/ExtUtils/xsubpp
  source /opt/gcc_env.sh

  ln -sf /$(pwd)/gpdb_src/gpAux/ext/${BLDARCH}/python-2.6.2 /opt/python-2.6.2
  export PATH=${JAVA_HOME}/bin:${PATH}
}

function generate_build_number() {
  pushd gpdb_src
    #Only if its git repro, add commit SHA as build number
    # BUILD_NUMBER file is used by getversion file in GPDB to append to version
    if [ -d .git ] ; then
      echo "commit:`git rev-parse HEAD`" > BUILD_NUMBER
    fi
  popd
}

function make_sync_tools() {
  pushd gpdb_src/gpAux
    make IVYREPO_HOST="${IVYREPO_HOST}" IVYREPO_REALM="${IVYREPO_REALM}" IVYREPO_USER="${IVYREPO_USER}" IVYREPO_PASSWD="${IVYREPO_PASSWD}" sync_tools
    # We have compiled LLVM with native zlib on CentOS6 and not from
    # the zlib downloaded from artifacts.  Therefore, remove the zlib
    # downloaded from artifacts in order to use the native zlib.
    find ext -name 'libz.*' -exec rm -f {} \;
  popd
}

function build_gpdb_and_scan_with_coverity() {
  pushd gpdb_src/gpAux
    make distclean
    cov-build --dir "$1" make BLD_TARGETS="gpdb" GPROOT=/usr/local
  popd
}

function _main() {
  # TODO: determine if these are needed
  prep_env_for_centos
  generate_build_number
  make_sync_tools

  /opt/prepare-coverity.bash
  build_gpdb_and_scan_with_coverity "$OUTPUT_ARTIFACT_DIR"
}

_main "$@"
