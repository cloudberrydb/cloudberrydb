#!/bin/bash
PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/conf.sh 

export HADOOP_HEAPSIZE=2048
hive -e "create database $HIVE_DB"
for data_file in $(ls $PWD/hive_data/); do
    echo "Loading $data_file..."
    hive -f "$PWD/hive_data/$data_file"
done
