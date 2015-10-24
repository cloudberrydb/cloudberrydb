#!/bin/bash

basePath=$("pwd")
pidFile="${basePath}/pgbouncer.pid"

if [ -f $pidFile ];
then
  pid=`cat ${pidFile}`
  kill $pid
else
  echo "Process file wasn't found. Aborting..."
fi