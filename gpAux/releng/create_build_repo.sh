#!/bin/bash

#
# Pulse clones the source tree to BLDWRAP_TOP, but our system needs it
# as BLDWRAP_TOP/src.  Since the source tree has its own src directory,
# we rename it to .orig and create an empty directory with the original name,
# then rename it as BLDWRAP_TOP/src.
#
# Another task here is to pull releng-build.  Only bin and lib directories
# are interesting to us, but we pull them to BLDWRAP_TOP/{bin,lib} to
# follow the build system rule.
#
# Pulse project should be configured RELENG_BUILD_URL to tell where to
# pull the releng-build.  You could add git options like -c in this property.
#

mv ${BLDWRAP_TOP} ${BLDWRAP_TOP}.orig

mkdir -p ${BLDWRAP_TOP}

mv ${BLDWRAP_TOP}.orig ${BLDWRAP_TOP}/src

cd ${BLDWRAP_TOP}

if [ -d "/tmp/releng-build" ]; then
    rm -rf /tmp/releng-build
fi

git clone ${RELENG_BUILD_URL} /tmp/releng-build

if [ -n "${RELENG_BUILD_BRANCH}" ]; then
    pushd /tmp/releng-build
    git checkout ${RELENG_BUILD_BRANCH}
    popd
fi

cp -r /tmp/releng-build/* /tmp/releng-build/.git ${BLDWRAP_TOP}

rm -rf /tmp/releng-build