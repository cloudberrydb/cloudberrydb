/*-------------------------------------------------------------------------
 *
 * loginmonitor.h
 *	  header file for integrated loginmonitor daemon
 *
 *
 * Copyright (c) 2023, Cloudberry Database, HashData Technology Limited.
 *
 * src/include/postmaster/loginmonitor.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGINMONITOR_H
#define LOGINMONITOR_H

#include "storage/block.h"

extern int StartLoginMonitorLauncher(void);
extern int StartLoginMonitorWorker(void);
extern Size LoginMonitorShmemSize(void);
extern void LoginMonitorShmemInit(void);
extern void SendLoginFailedSignal(const char *curr_user_name);
extern bool IsLoginMonitorWorkerProcess(void);
extern bool IsLoginMonitorLauncherProcess(void);
extern void HandleLoginFailed(void);
extern void LoginMonitorWorkerFailed(void);

#define IsAnyLoginMonitorProcess() \
	(IsLoginMonitorLauncherProcess() || IsLoginMonitorWorkerProcess())

extern int login_monitor_max_processes;
#endif							/* LOGINMONITOR_H */
