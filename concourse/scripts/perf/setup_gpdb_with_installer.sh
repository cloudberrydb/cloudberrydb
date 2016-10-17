#!/bin/bash

set -e -u

source $(dirname "$0")/common.sh

main() {
  check_config

  local INSTANCE_IDS=($(cat instance_ids/instance_ids.txt))
  local PRIVATE_IP=$(ec2-describe-instances --show-empty-fields ${INSTANCE_IDS} | grep INSTANCE | cut -f18) # Extract private IP

  prepare_for_ssh ${PRIVATE_IP}
  
  local INSTALLER_PATH="$(ls gpdb_installer/*.zip)"
  local INSTALLER_NAME="$(basename ${INSTALLER_PATH} .zip)"
  log "Found installer : ${INSTALLER_PATH}"

  log "Copying the ${INSTALLER_PATH} to ${PRIVATE_IP}"
  remote_push ${PRIVATE_IP} ${INSTALLER_PATH} /tmp

  log "Running the installer remotely..."
  remote_run_script ${PRIVATE_IP} root "INSTALLER_NAME=${INSTALLER_NAME}" $(dirname "$0")/install_gpdb.sh 
  remote_run_script ${PRIVATE_IP} root "INSTALLER_NAME=${INSTALLER_NAME} VERIFY=1" $(dirname "$0")/install_gpdb.sh 

  log "Starting GPDB cluster"
  remote_run_script ${PRIVATE_IP} gpadmin "" $(dirname "$0")/start_gpdb.sh 
  remote_run_script ${PRIVATE_IP} gpadmin "VERIFY=1" $(dirname "$0")/start_gpdb.sh 

  log "GPDB Installation complete!"
}

main "$@"
