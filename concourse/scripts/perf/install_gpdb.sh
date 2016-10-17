#!/bin/bash

set -e -u

function more() {
  cat $@ > /dev/null
}

main() {
  if [[ -z ${VERIFY:-} ]]; then
    apply
  else
    verify
  fi
}

apply() {
  export -f more
  local MAGIC="yes\n\nyes\nyes"
  unzip -o /tmp/${INSTALLER_NAME}.zip
  echo -e ${MAGIC} | ./${INSTALLER_NAME}.bin
  unset -f more
}

verify() {
  # assert the file /usr/local/greenplum-db/greenplum_path exists
  echo || [ -f /usr/local/greenplum-db/greenplum_path.sh ]
}

main "$@"
