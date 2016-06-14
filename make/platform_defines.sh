#!/bin/bash

GPOS_BIT=${GPOS_BIT:-GPOS_64BIT}

if [ -z "${GPOS_BIT}" ] || \
   [ "${GPOS_BIT}" = "GPOS_64BIT" ]; then
    ARCH_BIT=64
else
    ARCH_BIT=32
fi

ARCH_HARDWARE=${ARCH_HARDWARE:-x86}

PLAT=$( uname -s )
if [ $? -ne 0 ] ; then
    echo "Error executing uname -s"
    exit 1
fi

## ----------------------------------------------------------------------
## Solaris
## ----------------------------------------------------------------------

if [ ${PLAT} = "SunOS" ]; then

    PLATFORM_BASENAME="sol"
    CPU=`isainfo | awk '{print $1}'`
    OS_VERSION_MAJOR=`uname -r | awk -F. ' { print $2}'`

    if [ "${CPU}" = "sparcv9" ] ; then
        ARCH_HARDWARE="sparc"
    fi
fi

## ----------------------------------------------------------------------
## Linux (rhel & suse)
## ----------------------------------------------------------------------

if [ ${PLAT} = "Linux" ]; then


    if [ -r /etc/redhat-release ] ; then

        PLATFORM_BASENAME="rhel"
        OS_RELEASE=`cat /etc/redhat-release` 

        #Check for CentOS or Red Hat
        if grep 'Red Hat' /etc/redhat-release > /dev/null 2>&1 ; then
            OS_VERSION_MAJOR=`echo $OS_RELEASE | awk ' { print $7}' | awk -F "." '{print $1}'`
        else
            OS_VERSION_MAJOR=`echo $OS_RELEASE | awk ' { print $3}' | awk -F "." ' { print $1}'`
        fi
    fi

    if [ -r /etc/SuSE-release ]; then
        PLATFORM_BASENAME="suse"
        OS_VERSION_MAJOR=`head -2 /etc/SuSE-release | tail -1 | awk '{print $3}'`
    fi
fi

## ----------------------------------------------------------------------
## Mac OS X
## ----------------------------------------------------------------------

if [ ${PLAT} = "Darwin" ]; then
    PLATFORM_BASENAME="osx"
    sw_vers | grep ProductVersion | grep 10.5. > /dev/null 2>&1
    if [ $? = 0 ]; then
        OS_VERSION_MAJOR="105"
    else
        OS_VERSION_MAJOR="106"
    fi
fi

## ----------------------------------------------------------------------
## Assemble ARCH_BLD variable
## ----------------------------------------------------------------------

ARCH_BLD=${PLATFORM_BASENAME}${OS_VERSION_MAJOR}_${ARCH_HARDWARE}_${ARCH_BIT}
echo "${ARCH_BLD}"

exit 0

