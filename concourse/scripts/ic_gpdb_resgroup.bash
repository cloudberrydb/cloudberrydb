#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

basedir=/sys/fs/cgroup
options=rw,nosuid,nodev,noexec,relatime
groups="hugetlb freezer pids devices cpuset blkio net_prio net_cls cpuacct cpu memory perf_event"

# mount cgroup base dir
mkdir -p $basedir
mount -t tmpfs tmpfs $basedir

# mount cgroup controllers
for group in $groups; do
	mkdir -p $basedir/$group
	mount -t cgroup -o $options,$group cgroup $basedir/$group
done

mkdir -p $basedir/cpu/gpdb

# set all dirs' permission to 777 to allow test cases to control
# when and how cgroup is enabled
find $basedir -type d | xargs chmod 777

# do the actual job
${CWDIR}/ic_gpdb.bash
