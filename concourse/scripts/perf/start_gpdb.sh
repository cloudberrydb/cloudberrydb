#!/bin/bash

set -e

main() {
  if [[ -z ${VERIFY:-} ]]; then
    apply
  else
    verify
  fi
}

apply() {
  echo "source /usr/local/greenplum-db/greenplum_path.sh" >> ~gpadmin/.bashrc
  source /usr/local/greenplum-db/greenplum_path.sh

  local MASTER_HOSTNAME=perfhost
  local SEGMENT_HOSTS=perfhost
  local SEGMENTS_PER_HOST=3
  local MASTER_DIRECTORY=~gpadmin/master
  local SEGMENT_DIRECTORY=(~gpadmin/seg0 ~gpadmin/seg1 ~gpadmin/seg2)

  echo "Preparing ~gpadmin/gpconfigs"

  mkdir -p ~gpadmin/gpconfigs
  cp /usr/local/greenplum-db/docs/cli_help/gpconfigs/gpinitsystem_config ~gpadmin/gpconfigs/
  echo "$SEGMENT_HOSTS" > ~gpadmin/gpconfigs/hostfile_gpinitsystem

  sed -i -r "s|#MACHINE_LIST_FILE|MACHINE_LIST_FILE|" ~gpadmin/gpconfigs/gpinitsystem_config
  sed -i -r "s|MASTER_HOSTNAME=mdw|MASTER_HOSTNAME=${MASTER_HOSTNAME}|" ~gpadmin/gpconfigs/gpinitsystem_config
  sed -i -r "s|MASTER_DIRECTORY=.*|MASTER_DIRECTORY=${MASTER_DIRECTORY}|" ~gpadmin/gpconfigs/gpinitsystem_config
  sed -i -r "s| DATA_DIRECTORY=\(.*\)| DATA_DIRECTORY\=\(${SEGMENT_DIRECTORY[*]}\)|" ~gpadmin/gpconfigs/gpinitsystem_config

  echo "Creating master and segment data directories"
  mkdir -p ${MASTER_DIRECTORY} ${SEGMENT_DIRECTORY[*]}

  echo "Starting GPDB"
  gpinitsystem -a -c ~gpadmin/gpconfigs/gpinitsystem_config || [[ $? -lt 2 ]]
}

verify() {
  source /usr/local/greenplum-db/greenplum_path.sh
  psql template1 -c 'SELECT version();'
  psql template1 -c 'SELECT gp_opt_version();'
  psql template1 -c 'SET optimizer=ON'
}


main "$@"
