#!/bin/bash

if [ $# -ne 1 ];then 
    echo "Please specify etcd install dir!"
    exit -1
fi

target_path=$1
etcd_version=v3.3.25

if [[ `arch` = x86_64 ]]
then
    etcd_file_name=etcd-${etcd_version}-linux-amd64
else
    etcd_file_name=etcd-${etcd_version}-linux-arm64
fi

etcd_download_url=https://cbdb-deps.s3.amazonaws.com/etcd/${etcd_file_name}.tar.gz

wget ${etcd_download_url} -O /tmp/${etcd_file_name}.tar.gz
tar -xvf /tmp/${etcd_file_name}.tar.gz -C /tmp

mkdir -p ${target_path}
\cp  /tmp/${etcd_file_name}/etcd ${target_path}
\cp  /tmp/${etcd_file_name}/etcdctl ${target_path}
rm -rf /tmp/${etcd_file_name} /tmp/${etcd_file_name}.tar.gz
