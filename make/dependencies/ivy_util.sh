#!/bin/bash
## ======================================================================
## Post download untar trigger
## ======================================================================

FILE=$1
REVISION=$2

ARTIFACT_DIR=$( dirname ${FILE} )

##
## Expand tarball
##

if [ ! -f "${FILE}" ]; then
    echo "WARNING: tarball does not exist (${FILE})"
    exit 2
else
    pushd ${ARTIFACT_DIR}/..
    gunzip -qc ${FILE} | tar xf -
    popd

    if [ $? != 0 ]; then
        echo "FATAL: Problem exapanding tarball (${FILE})"
        exit 1
    fi

    ##
    ## Replace symlinks with actual files.  This is required by the
    ## cleancache command.  The java directory deletion has problems
    ## removing symbolic links.
    ##

    find ${ARTIFACT_DIR}/.. -type l -exec cp {} {}.tmp \; -exec rm {} \; -exec mv {}.tmp {} \;
fi

exit 0
