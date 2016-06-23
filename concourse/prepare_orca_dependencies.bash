#!/bin/bash

set -u -e -x

IVYFILE="gpdb_src/gpAux/releng/make/dependencies/ivy.xml"
PLATFORM="centos5"
BUILDTYPE="release"

setup_gpos() {
    gposdir=$1
    gposver=$(grep '\"libgpos\"' ${IVYFILE} | sed 's/.*rev="\([^"]*\)".*/\1/')
    wget -P $gposdir https://github.com/greenplum-db/gpos/releases/download/v$gposver/bin_gpos_${PLATFORM}_${BUILDTYPE}.tar.gz
}
setup_gporca() {
    gporcadir=$1
    gporcaver=$(grep '\"optimizer\"' ${IVYFILE} | sed 's/.*rev="\([^"]*\)".*/\1/')
    wget -P $gporcadir https://github.com/greenplum-db/gporca/releases/download/v$gporcaver/bin_orca_${PLATFORM}_${BUILDTYPE}.tar.gz
}

main() {
    setup_gpos bin_gpos
    setup_gporca bin_orca
}

main "$@"
