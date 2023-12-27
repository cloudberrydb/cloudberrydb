/*-------------------------------------------------------------------------
 *
 * pg_gophermeta.c
 *		gophermeta The local caching system is used to access
 * 		the object storage and the hdfs platform
 *
 *
 *	Copyright Hashdata Development Group
 *
 *	IDENTIFICATION
 *		contrib/pg_gophermeta/pg_gophermeta.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
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

#define GOPHERMETA_FOLDER "gophermeta"

static char workDir[MAXPGPATH + 1];

static char socketDir[MAXPGPATH + 1];

static char plasmaSocketDir[MAXPGPATH + 1];

/* Module load */
void _PG_init(void);
void gophermeta_main(Datum main_arg);
static void gophermeta_start_worker(void);
static void GetGopherMetaPath(char *dest);
static void GetGopherSocketPath(char *dest);
static void GetGopherPlasmaSocketPath(char *dest);

/* signal handler routines */
static void ReqShutdownHandler(SIGNAL_ARGS);
static void GopherSigHupHandler(SIGNAL_ARGS);

/* gopher routines */
static void GetGopherServerOssLogLevel(gopherOssServerConfig* ossConfig);

/* GUC variables */
static bool register_gophermeta = true;
static int gopher_local_capacity_mb = 10240;
static char *gopher_oss_log_level = NULL;
static char *gopher_oss_liboss2_log_level = NULL;

/*
 * Module load callback.
 */
void
_PG_init(void)
{
	DefineCustomIntVariable("pg_gophermeta.gopher_local_capacity_mb",
							"The local cache capacity in MB of GopherMeta process.",
							"Guc value the best-configuration set 30 percent of disk.",
							&gopher_local_capacity_mb,
							10240,
							1024, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("pg_gophermeta.gopher_oss_log_level",
							"Sets the Gopher OSS log severity.",
							"Valid values are DEBUG3, DEBUG2, DEBUG1, INFO, WARNING, ERROR, FATAL.",
							&gopher_oss_log_level,
							"WARNING",
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("pg_gophermeta.gopher_oss_liboss2_log_level",
							"Sets the Gopher OSS LIBOSS2 log severity.",
							"Valid values are DEBUG3, DEBUG2, DEBUG1, INFO, WARNING, ERROR, FATAL.",
							&gopher_oss_liboss2_log_level,
							"WARNING",
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* can't define PGC_POSTMASTER variable after startup */
	DefineCustomBoolVariable("pg_gophermeta.register_gophermeta",
							 "Starts the gophermeta worker.",
							 NULL,
							 &register_gophermeta,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);



	EmitWarningsOnPlaceholders("pg_gophermeta");

	if (register_gophermeta)
		gophermeta_start_worker();
}

/*
 * Start gophermeta worker process.
 */
static void
gophermeta_start_worker(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;

	strcpy(worker.bgw_library_name, "pg_gophermeta");
	strcpy(worker.bgw_function_name, "gophermeta_main");
	strcpy(worker.bgw_name, "gophermeta process");
	strcpy(worker.bgw_type, "gophermeta process");

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


static void
GetGopherMetaPath(char *dest)
{
	sprintf(dest, "%s/%s", DataDir, GOPHERMETA_FOLDER);
}

static void
GetGopherSocketPath(char *dest)
{
	sprintf(dest, ".s.gopher.%d", PostPortNumber);
}

static void
GetGopherPlasmaSocketPath(char *dest)
{
	sprintf(dest, ".s.gopher.plasma.%d", PostPortNumber);
}

void
gophermeta_main(Datum main_arg)
{
	sigjmp_buf  local_sigjmp_buf;
	MemoryContext gophermeta_context;

	int rc;

    /* Prepare folder path */
    GetGopherMetaPath(workDir);
	GetGopherSocketPath(socketDir);
	GetGopherPlasmaSocketPath(plasmaSocketDir);

	/*
     * Properly accept or ignore signals the postmaster might send us.
     *
     * Note: we deliberately ignore SIGTERM, because during a standard Unix
     * system shutdown cycle, init will SIGTERM all processes at once.  We
     * want to wait for the backends to exit, whereupon the postmaster will
     * tell us it's okay to shut down (via SIGUSR2).
     *
     * SIGUSR1 is presently unused; keep it spare in case someday we want this
     * process to participate in ProcSignal messaging.
     */
    pqsignal(SIGHUP, GopherSigHupHandler);  /* set flag to read config file */
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, SIG_IGN); /* ignore SIGTERM */
    pqsignal(SIGQUIT, quickdie);        /* hard crash time: nothing bg-writer specific, just use the standard */
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, SIG_IGN); /* reserve for ProcSignal */
    pqsignal(SIGUSR2, ReqShutdownHandler);      /* request shutdown */

	/*
     * Reset some signals that are accepted by postmaster but not here
     */
    pqsignal(SIGCHLD, SIG_DFL);
    pqsignal(SIGTTIN, SIG_DFL);
    pqsignal(SIGTTOU, SIG_DFL);
    pqsignal(SIGCONT, SIG_DFL);
    pqsignal(SIGWINCH, SIG_DFL);

	/* hashdata 3x default block sigquit */
	sigdelset(&BlockSig, SIGQUIT);

	/*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * TopMemoryContext, but resetting that would be a really bad idea.
     */
    gophermeta_context = AllocSetContextCreate(TopMemoryContext,
                                               "Gopher Meta",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(gophermeta_context);

	/*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        /* Since not using PG_TRY, must reset error stack by hand */
        error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* TODO(Gopher): Add cleanup code here if we catched exception here */
        /* Release Gopherwood context */

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(gophermeta_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(gophermeta_context);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1000000L);
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    BackgroundWorkerUnblockSignals();

    /* Check and create gophermeta working directory */
    DIR* dir = opendir(workDir);
    if (dir)
    {
        /* Directory exists. */
        closedir(dir);
    }
    else if (ENOENT == errno)
    {
        /* Directory does not exist. */
        mkdir(workDir, 0750);
    }
    else
    {
        int errnocode = errno;
        elog(WARNING, "Gopher Meta working directory %s create failed, errno=%d", workDir, errnocode);
        exit(1);
    }

    int64 capacity = (int64)gopher_local_capacity_mb * 1024 * 1024;
    /* Start Gopher Meta store service, it's an forever loop to response meta requests. */
    elog(LOG, "GopherMeta workDir = %s, gopher_local_capacity_mb=%d, capacity = %ld",
        workDir, gopher_local_capacity_mb, capacity);

    gopherOssServerConfig ossConfig;
    ossConfig.mCapacity = capacity;
	ossConfig.socketPath = NULL;
	ossConfig.socketName = socketDir;
	ossConfig.socketPlasmaName = plasmaSocketDir;
	ossConfig.ossUserCanceledCallBack = NULL;
    GetGopherServerOssLogLevel(&ossConfig);

    rc = gopherStartServer(workDir, &ossConfig, NULL, GOPHER_SERVER_WARNING, false);

    if (rc != 0)
    {
        elog(WARNING, "Gopher Meta Server(ContentId %d) got error!", GpIdentity.segindex);
    }

	if (!PostmasterIsAlive())
        exit(1);

    /* Normal exit from the bgwriter server is here */
    proc_exit(0);       /* done */
}

static void
GetGopherServerOssLogLevel(gopherOssServerConfig* ossConfig)
{
    if (gopher_oss_log_level) {
        if (pg_strcasecmp(gopher_oss_log_level, "DEBUG3") == 0) {
            ossConfig->logSeverity = GOPHER_SERVER_DEBUG3;
        } else if (pg_strcasecmp(gopher_oss_log_level, "DEBUG2") == 0) {
            ossConfig->logSeverity = GOPHER_SERVER_DEBUG2;
        } else if (pg_strcasecmp(gopher_oss_log_level, "DEBUG1") == 0) {
            ossConfig->logSeverity = GOPHER_SERVER_DEBUG1;
        } else if (pg_strcasecmp(gopher_oss_log_level, "INFO") == 0) {
            ossConfig->logSeverity = GOPHER_SERVER_INFO;
        } else if (pg_strcasecmp(gopher_oss_log_level, "WARNING") == 0) {
            ossConfig->logSeverity = GOPHER_SERVER_WARNING;
        } else if (pg_strcasecmp(gopher_oss_log_level, "ERROR") == 0) {
            ossConfig->logSeverity = GOPHER_SERVER_ERROR;
        } else if (pg_strcasecmp(gopher_oss_log_level, "FATAL") == 0) {
            ossConfig->logSeverity = GOPHER_SERVER_FATAL;
        } else {
            elog(ERROR, "Gopher OSS log severity not recognized : %s", gopher_oss_log_level);
        }
    }
    if (gopher_oss_liboss2_log_level) {
        if (pg_strcasecmp(gopher_oss_liboss2_log_level, "DEBUG3") == 0) {
            ossConfig->liboss2LogSeverity = GOPHER_SERVER_LIBOSS2_DEBUG3;
        } else if (pg_strcasecmp(gopher_oss_liboss2_log_level, "DEBUG2") == 0) {
            ossConfig->liboss2LogSeverity = GOPHER_SERVER_LIBOSS2_DEBUG2;
        } else if (pg_strcasecmp(gopher_oss_liboss2_log_level, "DEBUG1") == 0) {
            ossConfig->liboss2LogSeverity = GOPHER_SERVER_LIBOSS2_DEBUG1;
        } else if (pg_strcasecmp(gopher_oss_liboss2_log_level, "INFO") == 0) {
            ossConfig->liboss2LogSeverity = GOPHER_SERVER_LIBOSS2_INFO;
        } else if (pg_strcasecmp(gopher_oss_liboss2_log_level, "WARNING") == 0) {
            ossConfig->liboss2LogSeverity = GOPHER_SERVER_LIBOSS2_WARNING;
        } else if (pg_strcasecmp(gopher_oss_liboss2_log_level, "ERROR") == 0) {
            ossConfig->liboss2LogSeverity = GOPHER_SERVER_LIBOSS2_ERROR;
        } else if (pg_strcasecmp(gopher_oss_liboss2_log_level, "FATAL") == 0) {
            ossConfig->liboss2LogSeverity = GOPHER_SERVER_LIBOSS2_FATAL;
        } else {
            elog(ERROR, "Gopher OSS Liboss2 log severity not recognized : %s",
                 gopher_oss_liboss2_log_level);
        }
    }
}

/* SIGUSR2: set flag to run a shutdown checkpoint and exit */
static void
ReqShutdownHandler(SIGNAL_ARGS)
{
    /*
  	 * From here on, elog(ERROR) should end with exit(1), not send
  	 * control back to the sigsetjmp block above
  	 */
    ExitOnAnyError = true;

    gopherStopServer(workDir);
}

/* SIGHUP: re-read config file and update gopher server */
static void
GopherSigHupHandler(SIGNAL_ARGS)
{
    ProcessConfigFile(PGC_SIGHUP);
    gopherOption updateOptions;
    updateOptions.capacity = gopher_local_capacity_mb * 1024 * 1024;
    gopherUpdateGopherOption(&updateOptions);
}
