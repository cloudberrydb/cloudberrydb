#!/bin/bash

user=${1}

if test -z "$user"
then
    user="gpadmin"
fi

home_dir=/home/${user}

#auto-generated public ssh-key
if [ -f ${home_dir}/.ssh/id_rsa ]
then
  rm -rf ${home_dir}/.ssh/id_rsa
fi

if [ -f ${home_dir}/.ssh/id_rsa.pub ]
then
  rm -rf ${home_dir}/.ssh/id_rsa.pub
fi

su - ${user} -c "ssh-keygen -t rsa -N '' -f ${home_dir}/.ssh/id_rsa -q && cat ${home_dir}/.ssh/id_rsa.pub > ${home_dir}/.ssh/authorized_keys"

#execute auto-deploy for docker image.
su - ${user} -c "bash /home/${user}/workspace/cbdb/deploy/cbdb_deploy.sh build"

#running a infinite loop to avoid container killed by docker manager
tail -f /dev/null
