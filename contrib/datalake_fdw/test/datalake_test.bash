#set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source $BASE_DIR/create_env.sh

function start_regress_test() {
	cat > /opt/run_regress_test.sh <<-EOF
	trap look4diffs ERR

		function look4diffs() {

		    diff_files=\`find .. -name regression.diffs\`

		    for diff_file in \${diff_files}; do
			if [ -f "\${diff_file}" ]; then
			    cat <<-FEOF

						======================================================================
						DIFF FILE: \${diff_file}
						----------------------------------------------------------------------

						\$(cat "\${diff_file}")

					FEOF
			else
				echo "datalake regress ok"
				exit 0
			fi
		    done
		    exit 1
		}
		source /usr/local/cloudberry-db-devel/greenplum_path.sh
		export PGPORT=7000
		export COORDINATOR_DATA_DIRECTORY=/code/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1
		pushd "/code/gpdb_src/contrib/datalake_fdw/"
		(
			make > /dev/null 2>&1
			make installcheck
		)
		popd
	EOF

	chmod a+x /opt/run_regress_test.sh

	/opt/run_regress_test.sh

	if [ $? -ne 0 ]; then
		echo "datalake regress test failed!"
		exit 1
	fi
}

function start_tpcds_test() {
    source ${CBDB_INSTALL_DIRECTORY}/greenplum_path.sh
	$BASE_DIR/tpcds/run.sh
}

function load_data() {
	echo "hadoop leave saft mode"
	hadoop dfsadmin -safemode leave
	echo "load orc, parquet and avro data waiting..."
	export HADOOP_HEAPSIZE=2048
    date
    echo "begin load orc waiting..."
	hive -f $BASE_DIR/sql/hive_load_orc_data.sql > $BASE_DIR/hive_load_orc.log 2>&1
    date
    echo "begin load parquet waiting..."
	hive -f $BASE_DIR/sql/hive_load_parquet_data.sql > $BASE_DIR/hive_load_parquet.log 2>&1
	hive -f $BASE_DIR/sql/hive_load_avro_data.sql > $BASE_DIR/hive_load_avro.log 2>&1
	hive -f $BASE_DIR/sql/hive_load_empty_text_data.sql > $BASE_DIR/hive_load_empty_text_data.log 2>&1
}

function start_test() {
	build_env
	load_data
	start_regress_test
	start_tpcds_test
}

start_test
