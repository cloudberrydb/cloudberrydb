#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

function install_python_hacks() {
    # Our Python installation doesn't run standalone; it requires
    # LD_LIBRARY_PATH which causes virtualenv to fail (because the system and
    # vendored libpythons collide). We'll try our best to install patchelf to
    # fix this later, but it's not available on all platforms.
    if which yum > /dev/null; then
        yum install -y patchelf
    else
        set +x
        echo "ERROR: install_python_hacks() doesn't support the current platform and should be modified"
        exit 1
    fi
}

function install_python_requirements() {
    # virtualenv 16.0 and greater does not support python2.6, which is
    # used on centos6
    pip install --user virtualenv~=15.0
    export PATH=$PATH:~/.local/bin

    # create virtualenv before sourcing greenplum_path since greenplum_path
    # modifies PYTHONHOME and PYTHONPATH
    #
    # XXX Patch up the vendored Python's RPATH so we can successfully run
    # virtualenv. If we instead set LD_LIBRARY_PATH (as greenplum_path.sh
    # does), the system Python and the vendored Python will collide and
    # virtualenv will fail. This step requires patchelf.
    if which patchelf > /dev/null; then
        patchelf \
            --set-rpath /usr/local/greenplum-db-devel/ext/python/lib \
            /usr/local/greenplum-db-devel/ext/python/bin/python

        virtualenv \
            --python /usr/local/greenplum-db-devel/ext/python/bin/python /tmp/venv
    else
        # We don't have patchelf on this environment. The only workaround we
        # currently have is to set both PYTHONHOME and LD_LIBRARY_PATH and
        # pray that the resulting libpython collision doesn't break
        # something too badly.
        echo 'WARNING: about to execute a cross-linked virtualenv; here there be dragons'
        LD_LIBRARY_PATH=/usr/local/greenplum-db-devel/ext/python/lib \
        PYTHONHOME=/usr/local/greenplum-db-devel/ext/python \
        virtualenv \
            --python /usr/local/greenplum-db-devel/ext/python/bin/python /tmp/venv
    fi

    # Install requirements into the vendored Python stack.
    mkdir -p /tmp/py-requirements
    source /tmp/venv/bin/activate
        pip install --prefix /tmp/py-requirements -r ./gpdb_src/gpMgmt/requirements-dev.txt
        cp -r /tmp/py-requirements/* /usr/local/greenplum-db-devel/ext/python/
    deactivate
}

function gen_env(){
    cat > /opt/run_test.sh <<-EOF
		set -ex

		source /usr/local/greenplum-db-devel/greenplum_path.sh

		cd "\${1}/gpdb_src/gpAux"
		source gpdemo/gpdemo-env.sh

		cd "\${1}/gpdb_src/gpMgmt/"
		BEHAVE_TAGS="${BEHAVE_TAGS}"
		BEHAVE_FLAGS="${BEHAVE_FLAGS}"
		if [ ! -z "\${BEHAVE_TAGS}" ]; then
		    make -f Makefile.behave behave tags=\${BEHAVE_TAGS}
		else
		    flags="\${BEHAVE_FLAGS}" make -f Makefile.behave behave
		fi
	EOF

    chmod a+x /opt/run_test.sh
}

function setup_coverage() {
    # Enables coverage.py on this machine. Note that this function modifies
    # greenplum_path.sh, so callers need to source that file AFTER this is done.
    local commit_sha
    read -r commit_sha < ./gpdb_src/.git/HEAD
    local coverage_path="$(pwd)/coverage/$commit_sha"

    # This file will be copied into GPDB's PYTHONPATH; it sets up the coverage
    # hook for all Python source files that are executed.
    cat > /tmp/sitecustomize.py <<SITEEOF
import coverage
coverage.process_startup()
SITEEOF

    # Set up coverage.py to handle analysis from multiple parallel processes.
    cat > /tmp/coveragerc <<COVEOF
[run]
branch = True
data_file = $coverage_path/coverage
parallel = True
COVEOF

    # Now copy everything over to the installation.
    cp /tmp/sitecustomize.py /usr/local/greenplum-db-devel/lib/python
    cp /tmp/coveragerc /usr/local/greenplum-db-devel
    mkdir -p $coverage_path
    chown gpadmin:gpadmin $coverage_path

    # Enable coverage instrumentation after sourcing greenplum_path.
    echo 'export COVERAGE_PROCESS_START=/usr/local/greenplum-db-devel/coveragerc' >> /usr/local/greenplum-db-devel/greenplum_path.sh
}

function _main() {

    if [ -z "${BEHAVE_TAGS}" ] && [ -z "${BEHAVE_FLAGS}" ]; then
        echo "FATAL: BEHAVE_TAGS or BEHAVE_FLAGS not set"
        exit 1
    fi

    time install_gpdb
    time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash

    # Run inside a subshell so it does not pollute the environment after
    # sourcing greenplum_path
    time (make_cluster)

    time install_python_hacks
    time install_python_requirements
    time gen_env

    # Enable coverage.py.
    time setup_coverage

    time run_test

    time prepare_coverage "$TEST_NAME"
}

_main "$@"
