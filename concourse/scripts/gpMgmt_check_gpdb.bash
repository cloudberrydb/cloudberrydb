#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

function gen_env(){
  cat > /opt/run_test.sh <<-EOF

		base_path=\${1}
		RESULT_FILE="\${base_path}/gpdb_src/gpMgmt/gpMgmt_testunit_results.log"
		trap look4results ERR

		function look4results() {

		cat "\${RESULT_FILE}"
		exit 1
		}

		source /usr/local/greenplum-db-devel/greenplum_path.sh
		source /opt/gcc_env.sh
		source \${base_path}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
		cd \${base_path}/gpdb_src/gpMgmt/bin
		make check
		# show results into concourse
		cat \${RESULT_FILE}
	EOF

	chmod a+x /opt/run_test.sh
}

function setup_gpadmin_user() {
    ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash "$TEST_OS"
}

function _main() {

    # Run some of the following commands in a subshell so that they do not
    # pollute the environment after sourcing greenplum_path.
    (install_and_configure_gpdb)
    setup_gpadmin_user
    (make_cluster)

    # pip-install the gpMgmt requirements file.
    install_python_hacks
    install_python_requirements

    # Set up test coverage.
    setup_coverage

    gen_env
    run_test

    prepare_coverage "$TEST_NAME"
}

_main "$@"
