#!/bin/bash

destroy_gpdb() {
    # Setup environment
    source /usr/local/cloudberry-db-devel/greenplum_path.sh;
    export PGPORT=5432;
    export COORDINATOR_DATA_DIRECTORY=/data/gpdata/master/gpseg-1;

    yes|gpdeletesystem -f -d $COORDINATOR_DATA_DIRECTORY
    rm -rf $GPHOME/*
    rm -rf /home/gpadmin/gpdb_src/gpMgmt/bin/pythonSrc/ext/install
}
