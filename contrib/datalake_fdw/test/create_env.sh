function start_hdfs() {
echo "start hdfs server"
/usr/local/hadoop-3.3.4/sbin/start-all.sh
docker start mysql
nohup hive --service metastore > hive.out 2>&1 &
}

function create_configure() {
cat > /opt/gphdfs.conf <<-EOF
paa_cluster:
    # namenode host
    hdfs_namenode_host: localhost
    # name port
    hdfs_namenode_port: 9002
    # authentication method
    hdfs_auth_method: simple
    # rpc protection
    hadoop_rpc_protection: authentication
    # data Transfer Protocol
EOF

cat > /opt/gphive.conf <<-EOF
hive_cluster:
    uris: thrift://localhost:9083
    auth_method: simple
EOF

cp /opt/gphdfs.conf /code/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/
cp /opt/gphive.conf /code/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/
}

function install_lib() {
	echo "install thrift rpm"
	CWD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
	pushd ${CWD_DIR}
	sudo yum -y remove thrift.x86_64 thrift-lib-cpp.x86_64 thrift-lib-cpp-devel.x86_64
	wget https://artifactory.hashdata.xyz/artifactory/opensource-codes/thrift/rpms/thrift-0.12.0-el7.x86_64.rpm
	sudo rpm -i --prefix=/usr/local/cloudberry-db-devel/ thrift-0.12.0-el7.x86_64.rpm
	popd
}

function source_file() {
	source /usr/local/cloudberry-db-devel/greenplum_path.sh
	source /code/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
}

function load_vectorization() {
	echo "load vectorization"
	export PGPORT=7000
	export COORDINATOR_DATA_DIRECTORY=/code/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1
	gpstart -a
	gpconfig -c session_preload_libraries -v '${GPHOME}/lib/postgresql/vectorization.so'
	gpstop -ra
}

function build_env() {
	start_hdfs
	create_configure
	install_lib
	source_file
	load_vectorization
}
