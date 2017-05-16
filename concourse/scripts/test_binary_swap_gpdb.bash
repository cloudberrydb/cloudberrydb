#!/bin/bash -l

set -eox pipefail

function gen_env() {
    cat > /opt/run_test.sh <<-EOF
        source /usr/local/greenplum-db-devel/greenplum_path.sh
        source \${1}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
        cd "\${1}/gpdb_src/src/test/binary_swap"
        ./test_binary_swap.sh -b /tmp/local/greenplum-db-devel
EOF

    chmod a+x /opt/run_test.sh
}

function install_gpdb_binary_swap() {
    [ ! -d /tmp/local/greenplum-db-devel ] && mkdir -p /tmp/local/greenplum-db-devel
    tar -xzf binary_swap_gpdb/bin_gpdb.tar.gz -C /tmp/local/greenplum-db-devel
    sed -i -e "s/\/usr\/local/\/tmp\/local/" /tmp/local/greenplum-db-devel/greenplum_path.sh
    chown -R gpadmin:gpadmin /tmp/local
}

function run_test() {
    su - gpadmin -c "bash /opt/run_test.sh $(pwd)"
}

function _main() {
    time install_gpdb_binary_swap
    time gen_env
    time run_test
}

_main "$@"
