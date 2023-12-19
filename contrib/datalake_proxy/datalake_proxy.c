/*-------------------------------------------------------------------------
 *
 * datalake_proxy.c
 *		datalake_agent Access to the big data agent
 *
 *
 *	Copyright Hashdata Development Group
 *
 *	IDENTIFICATION
 *		gpcontrib/datalake_proxy/datalake_proxy.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <dirent.h>
#include <gopher/gopher_server.h>

#include "access/relation.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "libpq/pqsignal.h"
#include "storage/buf_internals.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/resowner.h"
#include "postmaster/postmaster.h"

PG_MODULE_MAGIC;


/* Module load */
void _PG_init(void);
void datalake_proxy_main(Datum main_arg);
static void datalake_proxy_start_worker(void);

/* Helper function */
static void startProxyProcess(pid_t pid);
static void DataLakeProxyLoop(pid_t pid);
static void DataLakeQuickdie(SIGNAL_ARGS);

bool IsUnderMasterDispatchMode(void);

/* GUC variables */
static bool register_datalake_proxy = true;

/* variables */
static volatile sig_atomic_t gotSIG = false;

int dlagent_memory_limit;

static void
DataLakeQuickdie(SIGNAL_ARGS)
{
	gotSIG = true;

	if(MyProc)
		SetLatch(&MyProc->procLatch);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* can't define PGC_POSTMASTER variable after startup */
	DefineCustomBoolVariable("datalake_proxy.register_datalake_proxy",
							 "Starts the datalake proxy worker.",
							 NULL,
							 &register_datalake_proxy,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("datalake_proxy.dlagent_memory_limit",
							 "Sets the maximum memory to be used for dlagent guc unit mb.",
							 "Sets dlagent memory limit.",
							 &dlagent_memory_limit,
							 2048,
							 512, MAX_KILOBYTES / 1024,
							 PGC_SIGHUP,
							 GUC_UNIT_MB,
							 NULL,
							 NULL,
							 NULL);


	EmitWarningsOnPlaceholders("datalake_proxy");

	if (register_datalake_proxy)
		datalake_proxy_start_worker();
}

bool
IsUnderMasterDispatchMode(void)
{
	if (Gp_role == GP_ROLE_DISPATCH)
		return true;

	return false;
}

static void
datalake_proxy_start_worker(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	if (!IsUnderMasterDispatchMode())
	{
		return;
	}

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;

	strcpy(worker.bgw_library_name, "datalake_proxy");
	strcpy(worker.bgw_function_name, "datalake_proxy_main");
	strcpy(worker.bgw_name, "datalake proxy process");
	strcpy(worker.bgw_type, "datalake proxy process");

	if (process_shared_preload_libraries_in_progress)
	{
		RegisterBackgroundWorker(&worker);
		return;
	}
	/* must set notify PID to wait for startup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
}

void
datalake_proxy_main(Datum main_arg)
{
	pid_t proxyPid;
	pid_t pid = getpid();

	pqsignal(SIGTERM, DataLakeQuickdie);
	pqsignal(SIGQUIT, DataLakeQuickdie);

	/* hashdata 3x default block sigquit */
	sigdelset(&BlockSig, SIGQUIT);

	switch (proxyPid = fork_process())
	{
		case -1:
			ereport(ERROR,
			        (errmsg("could not fork datalake proxy process: %m")));
			break;
		case 0:
			startProxyProcess(pid);

			/* if we're here an error occurred */
			exit(EXIT_FAILURE);
	}

	DataLakeProxyLoop(proxyPid);

	proc_exit(0);
}

static void
startProxyProcess(pid_t pid)
{
	int   i = 0;
	char *proxyArgs[10];
	char  parentPid[128];
	char  maxMemoryLimit[128];
	char  jarFile[MAXPGPATH];

	proxyArgs[i++] = "java";
	proxyArgs[i++] = "-Xms512m";

	snprintf(maxMemoryLimit, sizeof(maxMemoryLimit), "-Xmx%dm", dlagent_memory_limit);
	proxyArgs[i++] = maxMemoryLimit;

	proxyArgs[i++] = "-XX:+ExitOnOutOfMemoryError";

	proxyArgs[i++] = "-jar";
	snprintf(jarFile, sizeof(jarFile), "%s/java/dlagent-1.0.0.jar", pkglib_path);
	proxyArgs[i++] = jarFile;

	snprintf(parentPid, sizeof(parentPid), "--parent.pid=%d", pid);
	proxyArgs[i++] = parentPid;

	proxyArgs[i] = NULL;

	execvp("java", proxyArgs);

	ereport(ERROR, (errmsg("could not start proxy process: %m")));
}

static void
DataLakeProxyLoop(pid_t pid)
{
	int rc;

	while (true)
	{
		if (gotSIG)
		{
			gotSIG = false;
			kill(pid, SIGTERM);
			proc_exit(1);
		}

		if (waitpid(pid, NULL, WNOHANG) != 0)
			proc_exit(1);

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1 * 1000L, WAIT_EVENT_BGWORKER_STARTUP);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			kill(pid, SIGTERM);
			proc_exit(1);
		}
	}
}