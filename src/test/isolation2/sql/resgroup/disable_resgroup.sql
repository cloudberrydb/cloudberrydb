-- reset the GUC and restart cluster.
-- start_ignore
! gpconfig -r gp_resource_manager;
! gpstop -rai;
-- end_ignore

SHOW gp_resource_manager;
