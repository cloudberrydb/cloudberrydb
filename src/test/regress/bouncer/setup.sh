#!/bin/bash

basePath=$("pwd")
pidFile="${basePath}/pgbouncer.pid"
confFile="${basePath}/bouncer/config.ini"

if [ -z $PGPORT ]; then
    PGPORT=5432
fi

NEWPORT=$PGPORT
sed -e "s/BOUNCERPORT/$NEWPORT/g" ${confFile}_tpl > ${confFile}
if [ -f $pidFile ];
then
  echo "$pidFile already exists. Stop the process before attempting to start."
else
  #echo -n "" > $pidFile

    pgbouncer -d $confFile 
  #  echo -n "$! " >> $pidFile
fi
