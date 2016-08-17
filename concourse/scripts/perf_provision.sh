#!/bin/bash

set -e -u

source $(dirname "$0")/perf_common.sh

main() {
  check_config
  check_tools

  run_instances
  print_addresses

  log "Provision complete\n\n"

  # For demonstration only - the next story should remove these lines
  local PRIVATE_IP=$(ec2-describe-instances --show-empty-fields ${INSTANCE_IDS} | grep INSTANCE | cut -f18) # Extract private IP
  remote_run ${PRIVATE_IP} "echo 'Connected to VM!' && hostname"
}

check_tools() {
  if ! command -v ec2-run-instances >/dev/null 2>&1; then
    error "Amazon EC2 API Tools not installed (see http://aws.amazon.com/developertools/351)"
  fi
}


run_instances() {
  log "Starting instances"

  # If the instances contain any ephemeral volumes, this step is required to
  # set up device mappings. The number of ephemeral volumes depends on the
  # specific instance type. This does not fail if there are less than 24 such
  # volumes.
  BLOCK_DEV=(/dev/xvd{b..y})
  EPHEMERAL=(ephemeral{0..23})
  MAPPINGS=""
  for I in $(seq 0 23); do
    MAPPINGS+=" --block-device-mapping \"${BLOCK_DEV[$I]}=${EPHEMERAL[$I]}\""
  done

  INSTANCE_IDS=($(
    ec2-run-instances ${AMI} \
      -n 1 \
      --tenancy ${TENANCY} \
      --show-empty-fields \
      -k ${AWS_KEYPAIR} \
      --group ${SECURITY_GROUP_ID} \
      --subnet ${SUBNET_ID} \
      --instance-type ${INSTANCE_TYPE} \
      ${MAPPINGS} |
    grep INSTANCE |
    cut -f2
  ))
	echo ${INSTANCE_IDS} > instance_ids/instance_ids.txt

  log "Created instances: ${INSTANCE_IDS[*]}"

  local INSTANCE_ID=${INSTANCE_IDS}
  ec2-create-tags ${INSTANCE_ID} -t Name=${INSTANCE_NAME}

  wait_until_status "running"
  wait_until_check_ok

  local PRIVATE_IP=$(ec2-describe-instances --show-empty-fields ${INSTANCE_IDS} | grep INSTANCE | cut -f18) # Extract private IP
  prepare_for_ssh ${PRIVATE_IP}
}

prepare_for_ssh() {
  local IP=$1
  local KEYFILE="${AWS_KEYPAIR}.pem"

  log 'Preparing $IP for SSH'

  if [[ ! -f "${KEYFILE}" ]]; then
    echo "${AWS_KEYPAIR_VALUE}" > "${KEYFILE}"
    chmod 0600 "${KEYFILE}"
  fi

  mkdir -p ~/.ssh
  ssh-keyscan $IP >> ~/.ssh/known_hosts
}

print_addresses() {
  log "INSTANCE\tPRIVATE IP"
  ec2-describe-instances --show-empty-fields ${INSTANCE_IDS[*]} | grep INSTANCE | cut -f2,18
  log ""
}

main "$@"
