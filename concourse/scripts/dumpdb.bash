#!/bin/bash
set -e
set -u

# INSTALL_DIR is set manually in pipeline for release
INSTALL_DIR=${INSTALL_DIR:-/usr/local/cloudberry-db-devel}
source $INSTALL_DIR/greenplum_path.sh
source ./gpdb_src/gpAux/gpdemo/gpdemo-env.sh

# ignore ERR trap
gpstop -qa || :
gpstart -a
sleep 60
./gpdb_src/concourse/scripts/ic_start_fts_once.bash

psql \
    -X \
    -c "select datname from pg_database where datname != 'template0'" \
    --set ON_ERROR_STOP=on \
    --no-align \
    -t \
    --field-separator ' ' \
    --quiet \
    template1 | while read -r database ; do

    echo "-----------------------------------------------------------------"
    echo "Drop objects not shipped to customers from DATABASE: ${database}."
    echo "-----------------------------------------------------------------"
    psql "${database}" -f ./gpdb_src/concourse/scripts/drop_functions_with_dependencies_not_shipped_to_customers.sql
    echo ""
    echo ""
done

pg_dumpall -f ./sqldump/dump.sql
xz -z ./sqldump/dump.sql
