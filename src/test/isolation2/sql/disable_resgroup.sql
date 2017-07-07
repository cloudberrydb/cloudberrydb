-- for all removed resgroups the cgroup dirs should also be removed
! ls -d /sys/fs/cgroup/cpu/gpdb/*/;
! ls -d /sys/fs/cgroup/cpuacct/gpdb/*/;

-- reset the GUC and restart cluster.
-- start_ignore
! gpconfig -r gp_resource_manager;
! gpstop -rai;
-- end_ignore

SHOW gp_resource_manager;

-- reset admin_group concurrency setting
ALTER RESOURCE GROUP admin_group SET concurrency 10;
