#!/bin/bash
## ======================================================================
## ======================================================================

FLAGS_working_directory=$1
FLAGS_make_targets="$2"

## ----------------------------------------------------------------------

BLD_TOP=/opt/releng/build/${PULSE_PROJECT}/`basename ${PULSE_BASE_DIR}`

if [ ! -d "${BLD_TOP}" ]; then
    echo "FATAL: build directory does not exist (${BLD_TOP})"
    exit 1
fi

if [ ! -d "${BLD_TOP}/${FLAGS_working_directory}" ]; then
    echo "FATAL: working directory does not exist (${FLAGS_working_directory})"
    exit 1
fi

cd ${BLD_TOP}/${FLAGS_working_directory}

## ----------------------------------------------------------------------


if [ "${COVERITY_BUILD}" == "True" ]; then
    echo "starting cov"
    cov-build --dir ${BLD_TOP}/coverity make ${FLAGS_make_targets} 
    result_of_make=${PIPESTATUS[0]}

    if [ $result_of_make != 0 ]; then
        echo ""
        echo `date` "Error during compilation"
        buildDBUpdate failure "make error"
        echo "Exiting"
        exit 1
    else
        echo `date` "make finished."
       
        cov-analyze --dir ${BLD_TOP}/coverity \
            --security \
            --concurrency \
            --enable-constraint-fpp -j auto \
            --enable-virtual \
            --enable-callgraph-metrics \
            --enable-fnptr \
            --enable DIVIDE_BY_ZERO \
            --enable STACK_USE \
            --paths 10000 \
            --enable SWAPPED_ARGUMENTS \
            --enable RESOURCE_LEAK \
            --enable PARSE_ERROR \
            --enable BAD_SHIFT \
            --user-model-file ${BLDWRAP_TOP}/tools/coverity/pivotal_emc_model.xmldb \
            2>&1 | tee ${BLDWRAP_TOP}/tools/coverity/cov-analyze.log
       
        cov-commit-defects --dir=${BLD_TOP}/coverity \
            --host coverity.greenplum.com \
            --stream ${COVERITY_STREAM} \
            --user admin \
            --password 9c3962328041ddc65fab634e87f21de5 \
            --description "Build Project: ${PULSE_PROJECT}; Build Number: ${PULSE_BUILD_NUMBER}; Branch: ${PROJECT_BRANCH}; Version: ${PROJECT_VERSION} " \
            --target "${BLD_ARCH}" \
            --version "Commit: ${PULSE_BUILD_REVISION}" \
            2>&1 | tee ${BLDWRAP_TOP}/tools/coverity/cov-commit-defects.log

        ## Assigning owners to the defects
        if [ ! -L "/opt/ActivePerl-5.18" ]; then \
            ln -s /opt/releng/tools/perl/perl/5.18/rhel5_x86_64/perl /opt/ActivePerl-5.18 ;\
        fi
        export PATH=/opt/ActivePerl-5.18/bin:${PATH}
        which perl
        pushd ${BLD_TOP}/${FLAGS_working_directory} 
        perl ${BLDWRAP_TOP}/tools/coverity/assign-owner-to-unassigned-cids.pl \
        	--project "${COVERITY_PROJECT}" \
        	--config ${BLDWRAP_TOP}/tools/coverity/coverity_pse_config.xml \
        	--line-number --dry-run ${DRY_RUN} \
                2>&1 | tee ${BLDWRAP_TOP}/tools/coverity/assign-owner-to-unassigned-cids.log
        unlink /opt/ActivePerl-5.18
        popd

	export PYTHONPATH=/opt/releng/tools/Python/suds/0.4/noarch/suds/:${PYTHONPATH} 
	python ${BLDWRAP_TOP}/tools/coverity/findDefects.py \
		--host "coverity.greenplum.com" \
		--port "8080" \
		--user admin \
		--password 9c3962328041ddc65fab634e87f21de5 \
		--project "${COVERITY_PROJECT}" \
		--stream "${COVERITY_STREAM}" \
                --compareSnapshotId "${snapshotScope_compareSelector}" 2>&1 | tee -a ${BLDWRAP_TOP}/defects.log

    fi
        
else
        make ${FLAGS_make_targets}
        return=$?

        ## ----------------------------------------------------------------------

        echo ${FLAGS_make_targets} | grep test > /dev/null 2>&1
        if [ $? = 0 ]; then
            echo ${FLAGS_make_targets} | grep debug > /dev/null 2>&1
            if [ $? = 0 ]; then
            mkdir -p ${PULSE_BASE_DIR}/server/${ARCH_OUTPUT}.debug
            cp /opt/releng/build/${PULSE_PROJECT}/`basename ${PULSE_BASE_DIR}`/server/${ARCH_OUTPUT}.debug/test_log.txt \
                                     ${PULSE_BASE_DIR}/server/${ARCH_OUTPUT}.debug/
            else
            mkdir -p ${PULSE_BASE_DIR}/server/${ARCH_OUTPUT}.opt
            cp /opt/releng/build/${PULSE_PROJECT}/`basename ${PULSE_BASE_DIR}`/server/${ARCH_OUTPUT}.opt/test_log.txt \
                                     ${PULSE_BASE_DIR}/server/${ARCH_OUTPUT}.opt/
            fi
        fi
fi
## ----------------------------------------------------------------------

exit ${return}

