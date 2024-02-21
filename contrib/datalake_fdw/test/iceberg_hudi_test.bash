#!/bin/bash

set -euox pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

setup_docker_env() {
    su - gpadmin -c bash -- -e <<EOF
    echo "start iceberg-hudi docker environment"

    wget -nv https://artifactory.hashdata.xyz/artifactory/opensource-codes/apache/iceberg-hudi-docker.tar.gz
    tar -xzf iceberg-hudi-docker.tar.gz
    cd iceberg-hudi-docker
    ./setup_demo.sh
    sleep 10
EOF
}

load_hudi_data() {
    su - gpadmin -c bash -- -e <<EOF
    docker cp ${BASE_DIR}/hudi/hudi_load_data.sql adhoc-1:/opt/

    echo "load hudi test data"
    docker exec -i adhoc-1 spark-sql --jars https://artifactory.hashdata.xyz/artifactory/opensource-codes/apache/hudi-spark2.4-bundle_2.11-0.14.1.jar \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
    --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
    -f hudi_load_data.sql

    sleep 5
EOF
}

load_iceberg_data() {
    su - gpadmin -c bash -- -e <<EOF
    echo "load iceberg test data"
    docker cp iceberg-hudi-docker/iceberg-hive-runtime-1.4.2.jar hiveserver:/opt/hive/lib/
    docker restart hiveserver
    sleep 20
    docker cp ${BASE_DIR}/iceberg/iceberg_load_data.sql adhoc-1:/opt/
    docker exec -i adhoc-1 beeline -u jdbc:hive2://hiveserver:10000 -f iceberg_load_data.sql
EOF
}

run_test() {

echo "add hdfs and hive config to cluster"
cat > /opt/gphdfs.conf <<-EOF
hdfs-cluster-1:
    hdfs_namenode_host: 127.0.0.1
    hdfs_namenode_port: 8020
    hdfs_auth_method: simple
hive-cluster-1:
    uris: thrift://127.0.0.1:9083
    auth_method: simple
EOF

cat > /opt/gphive.conf <<-EOF
hive-cluster-1:
    uris: thrift://127.0.0.1:9083
    auth_method: simple
EOF

echo "127.0.0.1     namenode" >> /etc/hosts

    su - gpadmin -c bash -- -e <<EOF
    echo "start iceberg & hudi regress test"

    sudo chown gpadmin:gpadmin /opt/gphdfs.conf
    sudo chown gpadmin:gpadmin /opt/gphive.conf

    sudo cp -p /opt/gphdfs.conf ${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/
    sudo cp -p /opt/gphive.conf ${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/

    sudo cp -p /opt/gphdfs.conf ${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/
    sudo cp -p /opt/gphive.conf ${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/

    sudo cp -p /opt/gphdfs.conf ${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/dbfast2/demoDataDir1/
    sudo cp -p /opt/gphive.conf ${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/dbfast2/demoDataDir1/

    sudo cp -p /opt/gphdfs.conf ${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/dbfast3/demoDataDir2/
    sudo cp -p /opt/gphive.conf ${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/dbfast3/demoDataDir2/

    # source /usr/local/cloudberry-db-devel/greenplum_path.sh
    source /usr/local/cloudberry-db-devel/greenplum_path.sh
    export PGPORT=7000
    export COORDINATOR_DATA_DIRECTORY=${CBDB_SRC_DIRECTORY}/gpAux/gpdemo/datadirs/qddir/demoDataDir-1

    # install hive_connector
    cd ${CBDB_SRC_DIRECTORY}/contrib/hive_connector
    make
    make instal

    # install datalake agent
    cd ${CBDB_SRC_DIRECTORY}/contrib/datalake_agent
    make
    make install

    # install datalake_proxy
    cd ${CBDB_SRC_DIRECTORY}/contrib/datalake_proxy
    make
    make install

    cd ${CBDB_SRC_DIRECTORY}/contrib/datalake_fdw/
    make
    make install

    gpstop -ra
    sleep 5
    make installcheck ICEBERG_HUDI_TEST=1
EOF
}

main() {
   setup_docker_env
   load_hudi_data
   load_iceberg_data
   run_test
}

main
