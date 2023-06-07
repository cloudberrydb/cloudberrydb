#!/bin/bash
export PATH=$GPHOME/bin:$PATH
export PYTHONPATH=$GPHOME/lib/python:$PYTHONPATH
export LD_LIBRARY_PATH=$GPHOME/lib:$LD_LIBRARY_PATH

if [ -f /opt/rh/devtoolset-9/enable ]
then
  source /opt/rh/devtoolset-9/enable
fi

if [ -f $GPHOME/greenplum_path.sh ]
then
  source $GPHOME/greenplum_path.sh
fi

if [ -f ~/workspace/cbdb_dev/gpAux/gpdemo/gpdemo-env.sh ]
then
  source ~/workspace/cbdb_dev/gpAux/gpdemo/gpdemo-env.sh
fi

export PGHOST=127.0.0.1
export PGPORT=7000
export PGUSER=gpadmin
export PGDATABASE=postgres
export USER=gpadmin
