#!/bin/bash
set -e
set -u

INSTALL_DIR=${INSTALL_DIR:-/usr/local/cloudberry-db-devel}
source $INSTALL_DIR/greenplum_path.sh

gpfts -U gpadmin -W 2 > /tmp/gpfts_dump 2>&1