function p4_checkout_source_set_usage() {
    echo "Usage: checkout_source_set <depot> <release> <log file> <datestamp>"
    echo "If no branch is used, release needs to be set to none"
    exit 1
}


# requires DEPOT, RELEASE, LOG, and DATESTAMP be set by the caller
function checkout_source_set () {

    local DEPOT=$1
    local RELEASE=$2
    local LOG=$3
    local DATESTAMP=$4

    if [ x"${4}" = x ] ; then
        echo "Error: Incorrect number of arguments passed."
        p4_checkout_source_set_usage
    fi

    local P4CLIENT="build..${RELEASE}-${DATESTAMP}..`hostname`"
    local CLIENT_SPEC=client_spec

    # Create a workspace for this build
    echo "Client:	${P4CLIENT}"                                                 > ${CLIENT_SPEC}
    echo ""                                                                         >> ${CLIENT_SPEC}
    echo "Owner:	build"                                                      >> ${CLIENT_SPEC}
    echo ""                                                                         >> ${CLIENT_SPEC}
    echo "Host:	`hostname`"                                                         >> ${CLIENT_SPEC}
    echo ""                                                                         >> ${CLIENT_SPEC}
    echo "Description:	View for ${DATESTAMP} build of ${RELEASE}."                 >> ${CLIENT_SPEC}
    echo ""                                                                         >> ${CLIENT_SPEC}
    echo "Root:	`pwd`"                                                              >> ${CLIENT_SPEC}
    echo ""                                                                         >> ${CLIENT_SPEC}
    echo "Options:	noallwrite noclobber nocompress unlocked nomodtime normdir" >> ${CLIENT_SPEC}
    echo ""                                                                         >> ${CLIENT_SPEC}
    echo "SubmitOptions:	submitunchanged"                                    >> ${CLIENT_SPEC}
    echo ""                                                                         >> ${CLIENT_SPEC}
    echo "LineEnd:	local"                                                      >> ${CLIENT_SPEC}
    echo ""                                                                         >> ${CLIENT_SPEC}
    if [ "${RELEASE}" = "none" ] ; then
        echo "View:     //${DEPOT}/... //${P4CLIENT}/..."                                   >> ${CLIENT_SPEC}
    else
        echo "View:	//${DEPOT}/${RELEASE}/... //${P4CLIENT}/..."                        >> ${CLIENT_SPEC}
    fi

    cat ${CLIENT_SPEC} | p4 client -i > ${LOG} 2>&1
    if [ $? -ne 0 ]; then
        echo "`date` -- ERROR: Error occurred while creating Perforce client view spec."
        return 1
    fi

    p4 -c"${P4CLIENT}" sync -p >> ${LOG} 2>&1
    if [ $? -ne 0 ]; then
        echo "`date` -- ERROR: Error occurred while syncing source set from Perforce server."
        return 1
    fi

    p4 client -d "${P4CLIENT}" >> ${LOG} 2>&1
    if [ $? -ne 0 ]; then
        echo "`date` -- ERROR: Error occurred while deleting Perforce client view spec."
        return 1
    fi

    return 0
}
