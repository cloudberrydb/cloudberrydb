#!/bin/bash
set -eu

INSTALL_DIR=${INSTALL_DIR:-/usr/local/cloudberry-db-devel}
source $INSTALL_DIR/greenplum_path.sh

gpfts -U gpadmin -W 1 -L /tmp/gpfts_dump
