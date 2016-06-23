#!/bin/bash

set -u -e -x

setup_gpos() {
    gposdir=$1
    wget -P $gposdir https://github.com/greenplum-db/gpos/releases/download/v1.138/bin_gpos_centos5_release.tar.gz
}
setup_gporca() {
    gporcadir=$1
    wget -P $gporcadir https://github.com/greenplum-db/gporca/releases/download/v1.640/bin_orca_centos5_release.tar.gz
}

main() {
    yum -y install wget 

    setup_gpos bin_gpos
    setup_gporca bin_orca
}

main "$@"
