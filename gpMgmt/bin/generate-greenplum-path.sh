#!/usr/bin/env bash

if [ -z "$1" ]; then
  printf "Must specify a value for GPHOME"
  exit 1
fi

GPHOME_PATH="$1"
cat <<EOF
GPHOME="${GPHOME_PATH}"

EOF

cat <<"EOF"
PYTHONPATH="${GPHOME}/lib/python"
PATH="${GPHOME}/bin:${PATH}"
LD_LIBRARY_PATH="${GPHOME}/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
EOF

PLAT=`uname -s`
if [ $? -ne 0 ] ; then
    echo "Error executing uname -s"
    exit 1
fi

# AIX uses yet another library path variable
# Also, Python on AIX requires special copies of some libraries.  Hence, lib/pware.
if [ "${PLAT}" = "AIX" ]; then
cat <<"EOF"
PYTHONPATH="${GPHOME}/ext/python/lib/python2.7:${PYTHONPATH}"
LIBPATH="${GPHOME}/lib/pware:${GPHOME}/lib:${GPHOME}/ext/python/lib:/usr/lib/threads:${LIBPATH}"
export LIBPATH
GP_LIBPATH_FOR_PYTHON="${GPHOME}/lib/pware"
export GP_LIBPATH_FOR_PYTHON
EOF
fi

cat <<"EOF"

if [ -e "${GPHOME}/etc/openssl.cnf" ]; then
	OPENSSL_CONF="${GPHOME}/etc/openssl.cnf"
fi

export GPHOME
export PATH
export PYTHONPATH
export LD_LIBRARY_PATH
export OPENSSL_CONF
EOF
