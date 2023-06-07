#!/bin/bash
set -e
set -u

INSTALL_DIR=${INSTALL_DIR:-/usr/local/cloudberry-db-devel}
source $INSTALL_DIR/greenplum_path.sh

gpfts -U gpadmin -D -A 2>&1 