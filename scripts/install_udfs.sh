#!/bin/bash

#  This script manually installs a number of UDFs in the host database
#  system (PostgreSQL based). These UDFs are related to GP-ORCA and used 
#  mostly for debugging purpose.
#  
#  When GPDB or HAWQ is compiled with GP-ORCA, a shared library named
#  libgpoptudf.so (or libgpoptudf.dylib) is placed in $GPHOME/lib.
#  This shared library contains the definition (written in C) of these UDFs.
#  
#  In the future we could put all or part of these UDFs in gp_toolkit, 
#  such that these functions are available to use when the server is up.

#  To run this script, the database instance should be running and $GPHOME
#  should be defined in the environment and $GPHOME/greenplum_path.sh
#  should be sourced. The script needs to be run in the optimizer/scripts
#  folder since it takes load.sql.in as an input.

if [ $# -ne 1 ]; then
    echo "Usage: ./install_udfs.sh <database>"
    exit 1
fi

TARGET_DB=$1

if [ "${GPHOME}" = "" ]; then
	echo "Please source greenplum_path.sh. GPHOME must be set."
	exit 1; \
fi

echo "Using GPDB installed in ${GPHOME}"

UNAME=`uname`

if [ "Darwin" = "${UNAME}" ]; then
	LDSFX=dylib
elif [ "Linux" = "${UNAME}" ]; then
	LDSFX=so
else
	echo "Only Linux and Mac OS X are supported"
	exit 1
fi


GPOPTUTILS_NAMESPACE=gpoptutils

# replace library path to be installation specific
sed -e "s,%%CURRENT_DIRECTORY%%,${GPHOME}/lib,g; s,%%GPOPTUTILS_NAMESPACE%%,${GPOPTUTILS_NAMESPACE},g; s,%%EXT%%,${LDSFX},g" load.sql.in > /tmp/load.sql
# load functions into target database
psql $TARGET_DB -a -f /tmp/load.sql
