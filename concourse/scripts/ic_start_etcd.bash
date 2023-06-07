#!/bin/bash
set -e
set -u

INSTALL_DIR=${INSTALL_DIR:-/usr/local/cloudberry-db-devel}
source $INSTALL_DIR/greenplum_path.sh

ETCD_UNSUPPORTED_ARCH=arm64 nohup /bin/etcd 2>&1 &