/*-------------------------------------------------------------------------
 *
 * fts.c
 *	  Process under QD postmaster polls the segments on a periodic basis
 *    or at the behest of QEs.
 *
 * Maintains an array in shared memory containing the state of each segment.
 *
 * Portions Copyright (c) 2005-2010, Greenplum Inc.
 * Portions Copyright (c) 2011, EMC Corp.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/fts/fts.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <unistd.h>
#include "fts.h"
#include "port.h"
#include "common/ip.h"

#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "libpq-fe.h"

#include "ftsprobe.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "common/string.h"
#include "catalog/gp_configuration_history.h"
#include "catalog/gp_indexing.h"

#include "tcop/tcopprot.h" /* quickdie() */
#include "fe_utils/log.h"
#include "getopt_long.h"

#include <sys/param.h>			/* for MAXHOSTNAMELEN */
#include <pthread.h>
#include "fts_etcd.h"


const char *progname = NULL;
static void
usage()
{
	printf(_("%s - Fault Tolerance Server \n\n"), progname);
	printf(_("There is a fault prober process that is monitored from themaster/standby process and segments process.\n"));
	printf(_("This fault prober process is also called the FTS (fault tolerance server) process.\n"));
	printf(_("FTS runs independently of the postmaster, and can also update the cluster status whenthe master/standby fail.\n"));
	printf(_("Which means FTS won't restarted when master being unservice.\n\n"));
	printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
	printf(_("Options:\n"));

	printf(_("  -U, --user <user>                 Use the specified user to connect to the database.\n"
			 "                                    When FTS needs to update catalog(gp_segment_configuration),\n"
			 "                                    FTS will connect to the database through this user,\n"
			 "                                    default is current login user.\n"));
	printf(_("  -A, --one-round                   Skip the FTS loop probe, only a single probe check is performed,\n"
			 "                                    default is \"false\"\n"));
	printf(_("  -D, --disable-promote-standby     Not allow promote standby,\n"
			 "                                    default is \"false\"\n"));
	printf(_("  -R, --probe-retries <retries>     Probe number of retries,\n"
			 "                                    default is \"5\"\n"));
	printf(_("  -T, --probe-timeout <timeout>     Probe timeout(second),\n"
			 "                                    default is \"20\"\n"));
	printf(_("  -I, --probe-interval <interval>   FTS polling interval(second),\n"
			 "                                    default is \"60\"\n"));
	printf(_("  -v, --debug                       Enable debug log(lowest log level)\n"));
	printf(_("  -W, --warp-call <number>          FTS tools, The log level will be set debug level.\n"));
	printf(_("      --warp-call 1 -L <file>       Dump fts file into ETCD. -L specifies the FTS file\n" 
			 "                                    which will be written to ETCD. The written file will\n"
			 "                                    not be parsed, but directly overwritten.\n"));
	printf(_("      --warp-call 2                 Dump FTS info from ETCD. \n"));
	printf(_("      --warp-call 3                 Delete FTS info from ETCD. \n"));
	printf(_("      --warp-call 4                 Try write a fault value as `rewrite` message into postmaster.\n"
			 "                                    Used to verify whether the FTS node can connected to the postmaster\n"));
	printf(_("      --warp-call 5                 Dump standby_promote_ready info from ETCD. \n"
			 "                                    standby_promote_ready is a ETCD key used to confirm\n"
			 "                                    whether the standby node can be promoted safely.\n"));
	printf(_("  -h, -?, --help                    Show this help, then exit\n"));
	printf(_("  -F --etcd_conf_file               ETCD related configuration file path. \n"));
	printf(_("  -u --etcd_lease_timeout           ETCD ha lock lease timeout configuration. \n"));
	printf(_("  -d --log_directory                FTS log directory path. \n"));
	printf(_("  -n --log_rotate_line              FTS log maximum line for archieve rotating, default is \"5000\"\n"));
	printf(_("  -C --enable_k8s_compatible        FTS k8s deploy compatible functionality, default is \"false\"\n"));
}

static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{"warp-call", required_argument, NULL, 'W'},
		{"local-fts-file", required_argument, NULL, 'L'},
		{"user", optional_argument, NULL, 'U'},
		{"one-round", optional_argument, NULL, 'A'},
		{"disable-promote-standby", optional_argument, NULL, 'D'},
		{"probe-retries", optional_argument, NULL, 'R'},
		{"probe-timeout", optional_argument, NULL, 'T'},
		{"probe-interval", optional_argument, NULL, 'I'},
		{"debug", optional_argument, NULL, 'v'},
		{"etcd_conf_file", required_argument, NULL, 'F'},
		{"etcd_lease_timeout", optional_argument, NULL, 'u'},
		{"log_directory", optional_argument, NULL, 'd'},
		{"log_rotate_line", optional_argument, NULL, 'n'},
		{"fts_standalone_enabled", optional_argument, NULL, 'a'},
		{"fts_k8s_compatibility_enabled", optional_argument, NULL, 'C'},
		{NULL, 0, NULL, 0}
	};

#ifndef USE_INTERNAL_FTS

/*
 * STATIC VARIABLES
 */
static bool skip_fts_probe = false;
static int global_total_dbs = 0;
static CdbComponentDatabases *cdb_component_dbs = NULL;
static GpSegConfigEntry *entry_config = NULL;
static pthread_mutex_t etcd_update_lock = PTHREAD_MUTEX_INITIALIZER;

static int warp_call_i = -1;
static char * warp_local_fts_file;
static int fts_ha_lock_lease_timeout = FTS_HA_LOCK_LEASE_TIMEOUT_DEFAULT;
static char *etcd_account_id = GP_ETCD_ACCOUNT_ID_DEFAULT;
static char *etcd_cluster_id = GP_ETCD_CLUSTER_ID_DEFAULT;
static char *etcd_namespace = GP_ETCD_NAMESPACE_DEFAULT;
static char *etcd_endpoints = GP_ETCD_ENDPOINTS_DEFAULT;
static char fts_lock_path_key[FTS_ETCD_PATH_KEY_LEN];
static char fts_hostname_path_key[FTS_ETCD_PATH_KEY_LEN];
static char *fts_directory_name = NULL;
static bool fts_init_completed = false;

bool fts_k8s_compatibility_enabled = false;
bool fts_standalone_enabled = false;

/*
 * FUNCTION PROTOTYPES
 */
static void FtsLoop(fts_config * context_config);
static char *getCbdbConfPath(void);
static bool readGpEtcdConfigFromCbdbFile(char *);

static int
FtsLeaseTimedLock(int seconds)
{
	struct timespec time_out;
	clock_gettime(CLOCK_REALTIME, &time_out);
	time_out.tv_sec += seconds;
	return pthread_mutex_timedlock(&etcd_update_lock, &time_out);
}

static void*
FTSRenewLease(void *lease)
{
	int rc = 0;
	int rc_lock = 0;
	while (true)
	{
		rc_lock = FtsLeaseTimedLock(fts_ha_lock_lease_timeout*2);
		if (rc_lock != 0 && !fts_standalone_enabled)
		{
			cbdb_log_fatal("FTSRenewLease failed to get fts etcd lock with rc: %d.", rc_lock);
			exit(-1);
		}
		rc = renewFTSLeaseFromETCD(*(long long *)lease, fts_ha_lock_lease_timeout);
		pthread_mutex_unlock(&etcd_update_lock);
		if (rc != 0)
		{
			rc_lock = FtsLeaseTimedLock(fts_ha_lock_lease_timeout*2);
			cbdb_log_fatal("FTSRenewLease failed to renew lease for FTS lock rc: %d and force to exit fts process!", rc);
			exit(-1);
			if (rc_lock)
				pthread_mutex_unlock(&etcd_update_lock);
		}
		else
			cbdb_log_info("FTSRenewLease successfully to renew lease for FTS lock.");

		sleep(FTS_ETCD_LEASE_PROBE);
	}
}

static void
FTSLeaseThreadCreate(long long *lease)
{
	pthread_t thread;
	int status = 0;

	status = pthread_create(&thread, NULL, FTSRenewLease, (void *)lease);
	if (status!=0)
	{
		cbdb_log_fatal("FTSLeaseThreadCreate failed with error code: %d.",status);
		exit(-1);
	}
}

static bool createFTSLock(const char *key, char **lock, long long *lease, char *hostname, char *hostnamekey)
{
	bool getFtsLock = false;
	getFtsLock = getFTSLockFromETCD(key, lock, lease, fts_ha_lock_lease_timeout, hostname, hostnamekey);
	if (getFtsLock)
		FTSLeaseThreadCreate(lease);
	return getFtsLock;
}

static 
GpSegConfigEntry * readGpSegConfigFromETCD(int *total_dbs)
{
	char * buff = NULL;
	int rc;
	int	array_size = 500;
	int	idx = 0;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;

	char	hostname[MAXHOSTNAMELEN];
	char	address[MAXHOSTNAMELEN];
	char	datadir[4096];

	rc = readFTSDumpFromETCD(&buff);
	if (rc != 0)
	{
		cbdb_log_error("Failed to read FTS info from ETCD [rc=%d]", rc);
		return NULL;
	}

	configs = (GpSegConfigEntry *)palloc0(sizeof (GpSegConfigEntry) * array_size);

	char * ptr = strtok(buff, "\n");
	while (ptr)
	{
		config = &configs[idx];

		if (sscanf(ptr, "%d %d %c %c %c %c %d %s %s %s", (int *)&config->dbid, (int *)&config->segindex,
				   &config->role, &config->preferred_role, &config->mode, &config->status,
				   &config->port, hostname, address, datadir) != GPSEGCONFIGNUMATTR)
		{
			pfree(buff);
			pfree(configs);
			cbdb_log_error("invalid data in gp_segment_configuration from ETCD.(content=%d, dbid=%d)", config->dbid, config->dbid);
			exit(-1);
		}


		config->hostname = pstrdup(hostname);
		config->address = pstrdup(address);
		config->datadir = pstrdup(datadir);

		idx++;

		if (idx >= array_size)
		{
			array_size = array_size * 2;
			configs = (GpSegConfigEntry *)
				repalloc(configs, sizeof(GpSegConfigEntry) * array_size);
		}

		ptr = strtok(NULL, "\n");
	}

	pfree(buff);
	*total_dbs = idx;

	return configs;
}

static char *
getCbdbConfPath(void)
{
	char *path = (char *)palloc0(sizeof(char)*FTS_CBDB_PATH_LEN);
	memset(path, 0, FTS_CBDB_PATH_LEN);
	char *ret = getenv(FTS_GPHOME_PATH);
	if (ret == NULL || strcmp(ret, "") == 0)
	{
		cbdb_log_fatal("Error: failed to get cbdb conf path from environment configuration!");
		return NULL;
	}
	asprintf(&path, "%s/%s", ret, FTS_ETCD_CONF_FILE_NAME);
	cbdb_log_info("FTS getCbdbConfPath successfully get environment configuration path: %s.", path);
	return path;
}

static char *
getCbdbLogPath(void)
{
	char *path = (char *)palloc0(sizeof(char)*FTS_CBDB_PATH_LEN);
	memset(path, 0, FTS_CBDB_PATH_LEN);
	char *ret = getenv(FTS_CBDB_PATH);
	if (ret == NULL || strcmp(ret, "") == 0)
	{
		asprintf(&path, "%s", FTS_LOG_FILE_DIRECTORY_TMP);
	}
	else
	{
		asprintf(&path, "%s/%s", ret, FTS_LOG_FILE_DIRECTORY_DEFAULT);
	}
	return path;
}

static bool
checkValueFormat(char str[])
{
	bool result = false;
	size_t len = strlen(str);
	if (str[0] == '\'' && str[len-1] == '\'' )
	{
		return true;
	}
	return result;
}

static void
tailQuotationMark(char *input, char **output, size_t len)
{
	Assert(len >= 2);
	memmove(input, input+1, len-2);
	input[len-2] = '\0';
	*output = strdup(input);
}

static bool
readGpEtcdConfigFromCbdbFile(char *confFileName)
{
	FILE	*fd = NULL;
	char	buf[128];
	bool ret = false;
	char *fileName = NULL;

	if (confFileName == NULL || confFileName[0] == '\0')
		fileName = getCbdbConfPath();
	else
		fileName = confFileName;

	Assert((fileName != NULL) && (strcmp(fileName, "") != 0));

	fd = fopen(fileName, "r");
	if (!fd)
	{
		cbdb_log_fatal("FTS readGpEtcdConfigFromCbdbFile Could not open cbdb configuration file:%s.", fileName);
		goto error_handle;
	}

	while (fgets(buf, sizeof(buf), fd))
	{
		char key[FTS_HOSTNAME_LEN];
		char value[FTS_HOSTNAME_LEN];
		char *value_ptr = NULL;

		if (buf[0] == '\0' || buf[0] == '\n')
			continue;

		if (sscanf(buf, "%[^'=']=%s", key, value) != GPETCDCONFIGNUMATTR)
		{
			cbdb_log_fatal("FTS readGpEtcdConfigFromCbdbFile Invalid data in config file: %s.", fileName);
			goto error_handle;
		}

		if (checkValueFormat(value) == false)
		{
			cbdb_log_fatal("readGpEtcdConfigFromCbdbFile input parameter %s parsing failed.", value);
			goto error_handle;
		}

		tailQuotationMark(value, &value_ptr, strlen(value));
		if (strncmp(key, "gp_etcd_account_id", strlen(key)) == 0)
			etcd_account_id = pstrdup(value_ptr);
		else if (strncmp(key, "gp_etcd_cluster_id", strlen(key)) == 0)
			etcd_cluster_id = pstrdup(value_ptr);
		else if (strncmp(key, "gp_etcd_namespace", strlen(key)) == 0)
			etcd_namespace = pstrdup(value_ptr);
		else if (strncmp(key, "gp_etcd_endpoints", strlen(key)) == 0)
			etcd_endpoints = pstrdup(value_ptr);

		if (etcd_account_id == NULL || etcd_cluster_id == NULL ||etcd_namespace == NULL || etcd_endpoints == NULL)
		{
			cbdb_log_fatal("FTS readGpEtcdConfigFromCbdbFile Error failed to read configuration from file: %s.", fileName);
			goto error_handle;
		}
	}

	memset(fts_lock_path_key, 0, FTS_ETCD_PATH_KEY_LEN);
	memset(fts_hostname_path_key, 0, FTS_ETCD_PATH_KEY_LEN);
	snprintf(fts_lock_path_key, FTS_ETCD_PATH_KEY_LEN, "%s/%s/%s/%s/%s",
		FTS_METADATA_DIR_PREFIX, etcd_namespace, etcd_account_id, etcd_cluster_id, FTS_ETCD_HA_LOCK_KEY);
	snprintf(fts_hostname_path_key, FTS_ETCD_PATH_KEY_LEN, "%s/%s/%s/%s/%s",
		FTS_METADATA_DIR_PREFIX, etcd_namespace, etcd_account_id, etcd_cluster_id , FTS_ETCD_HA_HOSTNAME_KEY);

	if (fts_lock_path_key == NULL || strcmp(fts_lock_path_key, "") == 0 || fts_hostname_path_key == NULL || strcmp(fts_hostname_path_key, "") == 0)
		goto error_handle;

	cbdb_log_info("FTS readGpEtcdConfigFromCbdbFile read configuration from file: %s, etcd_account_id: %s, etcd_cluster_id: %s, etcd_namespace: %s, etcd_endpoints: %s \
		FTS HA lock path: %s, FTS Master node key: %s.",fileName, etcd_account_id, etcd_cluster_id, etcd_namespace, etcd_endpoints, fts_lock_path_key, fts_hostname_path_key);

	ret = true;
error_handle:
	if (fileName)
		pfree(fileName);
	if (fd)
		fclose(fd);
	return ret;
}

static char *
getDnsAddress(char *name, int port)
{
	char			hostinfo[NI_MAXHOST];

	int			ret;
	char		portNumberStr[32];
	char	   *service;
	struct addrinfo *addrs = NULL, *addr;
	struct addrinfo hint;

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;

	snprintf(portNumberStr, sizeof(portNumberStr), "%d", port);
	service = portNumberStr;

	ret = pg_getaddrinfo_all(name, service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);

		/*
		 * If a host name is unknown, whether it is an error depends on its role:
		 * - if it is a primary then it's an error;
		 * - if it is a mirror then it's just a warning;
		 * but we do not know the role information here, so always treat it as a
		 * warning, the callers should check the role and decide what to do.
		 */
		cbdb_log_error("could not translate host name \"%s\", port \"%d\" to address: %s",
							name, port, gai_strerror(ret));

		return NULL;
	}

	hostinfo[0] = '\0';
	for (addr = addrs; addr; addr = addr->ai_next)
	{
		/* Ignore AF_UNIX sockets, if any are returned. */
		if (addr->ai_family == AF_UNIX)
			continue;

		if (addr->ai_family == AF_INET) /* IPv4 address */
		{
			MemSet(hostinfo, 0, sizeof(hostinfo));
			pg_getnameinfo_all((struct sockaddr_storage *) addr->ai_addr, addr->ai_addrlen,
								hostinfo, sizeof(hostinfo),
							   NULL, 0,
							   NI_NUMERICHOST);

			break;
		}
	}

	if (!hostinfo[0] && addrs->ai_family == AF_INET6)
	{
		addr = addrs;
		/* Get a text representation of the IP address */
		pg_getnameinfo_all((struct sockaddr_storage *) addr->ai_addr, addr->ai_addrlen,
							   hostinfo, sizeof(hostinfo),
							   NULL, 0,
							   NI_NUMERICHOST);

	}
	pg_freeaddrinfo_all(hint.ai_family, addrs);
	return pg_strdup(hostinfo);
}

static int
CdbComponentDatabaseInfoCompare(const void *p1, const void *p2)
{
	const CdbComponentDatabaseInfo *obj1 = (CdbComponentDatabaseInfo *) p1;
	const CdbComponentDatabaseInfo *obj2 = (CdbComponentDatabaseInfo *) p2;

	int			cmp = obj1->config->segindex - obj2->config->segindex;

	if (cmp == 0)
	{
		int			obj2cmp = 0;
		int			obj1cmp = 0;

		if (SEGMENT_IS_ACTIVE_PRIMARY(obj2))
			obj2cmp = 1;

		if (SEGMENT_IS_ACTIVE_PRIMARY(obj1))
			obj1cmp = 1;

		cmp = obj2cmp - obj1cmp;
	}

	return cmp;
}

static void
cleanCdbComponentInfos()
{
	if (entry_config)
	{
		GpSegConfigEntry *config;
		for (int i = 0; i < global_total_dbs; i++)
		{
			config = &entry_config[i];
			pfree(config->hostip);
			pfree(config->hostaddrs[0]);
			pfree(config->hostname);
			pfree(config->address);
			pfree(config->datadir);
		}

		pfree(entry_config);
		entry_config = NULL;
	}

	if (cdb_component_dbs) 
	{
		pfree(cdb_component_dbs->segment_db_info);
		pfree(cdb_component_dbs->entry_db_info);
		pfree(cdb_component_dbs);
		cdb_component_dbs = NULL;
	}
}

void
initCdbComponentDatabases(GpSegConfigEntry * configs, int total_dbs)
{
	CdbComponentDatabaseInfo *cdbInfo;
	int i;

	if (!total_dbs) 
	{
		cbdb_log_error("no segmentes infos.");
		goto error;
	}

	if (!cdb_component_dbs) 
	{
		cdb_component_dbs = (CdbComponentDatabases *)palloc0(sizeof(CdbComponentDatabases));
		cdb_component_dbs->segment_db_info = (CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * total_dbs);
		cdb_component_dbs->entry_db_info = (CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * 2);
	}
	
	cdb_component_dbs->numActiveQEs = 0;
	cdb_component_dbs->numIdleQEs = 0;
	cdb_component_dbs->qeCounter = 0;
	cdb_component_dbs->freeCounterList = NIL;

	for (i = 0; i < total_dbs; i++)
	{
		CdbComponentDatabaseInfo	*pRow;
		GpSegConfigEntry	*config = &configs[i];

		/* lookup hostip/hostaddrs cache */
		config->hostip= NULL;
		config->hostaddrs[0] = getDnsAddress(config->address, config->port);

		/* fts_k8s_compatibility_enabled is used to be compatible with the k8s environment
		  * in case dynamic dns resolve failure caused by following cases:
		  * 1. when initializing FTS service adn DDNS server not ready yet, then retry and wait for DDNS ready.
		  * 2. when node in failover state, then mark the related node with FTS_PROBE_FAILED status, do not exit
		  *     and wait for next round FTS probe processing.
		*/
		if (fts_k8s_compatibility_enabled)
		{
			if (!fts_init_completed && !config->hostaddrs[0])
			{
				for (int i = 0; i <= FTS_DNS_WAIT_TIMEOUT; i++)
				{
					cbdb_log_warning("FTS initialization retry to initCdbComponentDatabases!");
					config->hostaddrs[0] = getDnsAddress(config->address, config->port);
					if (config->hostaddrs[0])
						break;
					sleep(1);
				}
			}

			if (config->hostaddrs[0] != NULL)
				config->hostip = pg_strdup(config->hostaddrs[0]);
			else
				config->hostip = pg_strdup("");
		}
		else
		{
			/*
			* We make sure we get a valid hostip for primary here,
			* if hostip for mirrors can not be get, ignore the error.
			*/
			if (config->hostaddrs[0] == NULL &&
				config->role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
			{
				cbdb_log_error("cannot resolve network address for dbid=%d", config->dbid);
				goto error;
			}

			if (config->hostaddrs[0] != NULL)
				config->hostip = pg_strdup(config->hostaddrs[0]);
		}

		/*
		 * Determine which array to place this rows data in: entry or segment,
		 * based on the content field.
		 */
		if (config->segindex >= 0)
		{
			pRow = &cdb_component_dbs->segment_db_info[cdb_component_dbs->total_segment_dbs];
			cdb_component_dbs->total_segment_dbs++;
		}
		else
		{
			pRow = &cdb_component_dbs->entry_db_info[cdb_component_dbs->total_entry_dbs];
			cdb_component_dbs->total_entry_dbs++;
		}

		pRow->cdbs = cdb_component_dbs;
		pRow->config = config;
		pRow->freelist = NIL;
		pRow->numIdleQEs = 0;
		pRow->numActiveQEs = 0;
	}

	/*
	 * Validate that there exists at least one entry and one segment database
	 * in the configuration
	 */
	if (cdb_component_dbs->total_segment_dbs == 0)
	{
		cbdb_log_error("number of segment databases cannot be 0");
		goto error;
	}

	if (cdb_component_dbs->total_entry_dbs == 0)
	{
		cbdb_log_error("number of entry databases cannot be 0");
		goto error;
	}

	/*
	 * Now sort the data by segindex, isprimary desc
	 */
	pg_qsort(cdb_component_dbs->segment_db_info,
		  cdb_component_dbs->total_segment_dbs, sizeof(CdbComponentDatabaseInfo),
		  CdbComponentDatabaseInfoCompare);

	pg_qsort(cdb_component_dbs->entry_db_info,
		  cdb_component_dbs->total_entry_dbs, sizeof(CdbComponentDatabaseInfo),
		  CdbComponentDatabaseInfoCompare);

	
	/*
	 * Now count the number of distinct segindexes. Since it's sorted, this is
	 * easy.
	 */
	for (i = 0; i < cdb_component_dbs->total_segment_dbs; i++)
	{
		if (i == 0 ||
			(cdb_component_dbs->segment_db_info[i].config->segindex 
				!= cdb_component_dbs->segment_db_info[i - 1].config->segindex))
		{
			cdb_component_dbs->total_segments++;
		}
	}

	/*
	 * Now validate that our identity is present in the entry databases
	 */
	for (i = 0; i < cdb_component_dbs->total_entry_dbs; i++)
	{
		cdbInfo = &cdb_component_dbs->entry_db_info[i];

		if (cdbInfo->config->preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY
			&& cdbInfo->config->segindex == MASTER_CONTENT_ID)
		{
			break;
		}
	}

	if (i == cdb_component_dbs->total_entry_dbs)
	{
		cbdb_log_error("Cannot locate entry database, Entry database represented by this db in gp_segment_configuration: role %c content %d",
						   GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY, MASTER_CONTENT_ID);
		goto error;
	}

	/*
	 * Now validate that the segindexes for the segment databases are between
	 * 0 and (numsegments - 1) inclusive, and that we hit them all.
	 * Since it's sorted, this is relatively easy.
	 */
	int x = 0;
	for (i = 0; i < cdb_component_dbs->total_segments; i++)
	{
		int			this_segindex = -1;

		while (x < cdb_component_dbs->total_segment_dbs)
		{
			this_segindex = cdb_component_dbs->segment_db_info[x].config->segindex;
			if (this_segindex < i)
				x++;
			else if (this_segindex == i)
				break;
			else if (this_segindex > i)
			{
				cbdb_log_error("Content values must be in the range 0 to %d inclusive.",
								   cdb_component_dbs->total_segments - 1);
				goto error;
			}
		}
		if (this_segindex != i)
		{
			cbdb_log_error("Content values must be in the range 0 to %d inclusive.",
								   cdb_component_dbs->total_segments - 1);
				goto error;
		}
	}

	char * master_host_ip = NULL;
	for (i = 0; i < cdb_component_dbs->total_entry_dbs; i++)
	{
		cdbInfo = &cdb_component_dbs->entry_db_info[i];

		if (cdbInfo->config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY 
			|| cdbInfo->config->hostip == NULL)
			continue;

		master_host_ip = cdbInfo->config->hostip;
		
		int count = 1;
		CdbComponentDatabaseInfo *cdbInfo_seg;
		for (int j = 0; j < cdb_component_dbs->total_segment_dbs; j++)
		{
			if (cdbInfo->config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY 
				|| cdbInfo->config->hostip == NULL)
				continue;

			cdbInfo_seg = &cdb_component_dbs->segment_db_info[j];
			if (strncmp(cdbInfo->config->hostip, cdbInfo_seg->config->hostip, strlen(cdbInfo->config->hostip)) == 0)
			{
				++count;
			}
		}

		cdbInfo->hostPrimaryCount = count;
	}

	for (i = 0; i < cdb_component_dbs->total_segment_dbs; i++)
	{
		cdbInfo = &cdb_component_dbs->segment_db_info[i];

		if (cdbInfo->config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY || cdbInfo->config->hostip == NULL)
			continue;

		int count = 0;
		CdbComponentDatabaseInfo *cdbInfo_seg;

		if (master_host_ip &&
			strncmp(cdbInfo->config->hostip, master_host_ip, strlen(cdbInfo->config->hostip)) == 0)
		{
			++count;
		}

		for (int j = 0; j < cdb_component_dbs->total_segment_dbs; j++)
		{
			if (cdbInfo->config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY 
				|| cdbInfo->config->hostip == NULL)
				continue;
			cdbInfo_seg = &cdb_component_dbs->segment_db_info[j];
			if (strncmp(cdbInfo->config->hostip, cdbInfo_seg->config->hostip, strlen(cdbInfo->config->hostip)) == 0)
			{
				++count;
			}
		}

		cdbInfo->hostPrimaryCount = count;
	}

	return;
error:
	cleanCdbComponentInfos();
}

/*
 * Populate cdb_component_dbs object by reading from catalog.
 * Internally, the object is allocated in CdbComponentsContext.
 */
static bool 
initCdbComponentInfosAndUpdateStatus()
{
	entry_config = readGpSegConfigFromETCD(&global_total_dbs);
	if (!entry_config || global_total_dbs == 0)
	{
		cleanCdbComponentInfos();
		return true;
	}

	initCdbComponentDatabases(entry_config, global_total_dbs);
	if (!cdb_component_dbs)
	{
		cleanCdbComponentInfos();
		exit(0);
	}

	return false;
}

static
void FtsLoop(fts_config * context_config)
{
	bool updated_probe_state;
	bool skip_current_round = false;
	time_t elapsed,	probe_start_time, timeout;
	char fts_hostname[FTS_HOSTNAME_LEN];
	gethostname(fts_hostname, FTS_HOSTNAME_LEN);
	bool getFtsLock = false;
	char *lock = NULL;
	long long lease = 0;

	if (context_config->one_round)
	{
		getFtsLock = true;
	}
	else
	{
		getFtsLock = createFTSLock(fts_lock_path_key, &lock, &lease, fts_hostname, fts_hostname_path_key);
	}
 
	while (true)
	{
		if (getFtsLock)
		{
			fts_init_completed = true;
			probe_start_time = time(NULL);
			pthread_mutex_lock(&etcd_update_lock);
			skip_current_round = initCdbComponentInfosAndUpdateStatus();

			if (skip_current_round)
				cbdb_log_warning("skipped current round. ");
			else
			{
				cbdb_log_info("starting scan with %d segments and %d contents and master group", 
					cdb_component_dbs->total_segment_dbs, 
					cdb_component_dbs->total_segments);
				updated_probe_state = FtsWalRepMessageSegments(cdb_component_dbs, context_config);
			}

			cleanCdbComponentInfos();
			pthread_mutex_unlock(&etcd_update_lock);

			if (unlikely(context_config->one_round))
				return;

			/* check if we need to sleep before starting next iteration */
			elapsed = time(NULL) - probe_start_time;
			timeout = elapsed >= context_config->probe_interval ? 0 : 
							context_config->probe_interval - elapsed;

			sleep(timeout);
		}
		else
		{
			lease = 0;
			getFtsLock = createFTSLock(fts_lock_path_key, &lock, &lease, fts_hostname, fts_hostname_path_key);
			sleep(FTS_ETCD_LOCK_RETRY_WAIT);
		}
	} /* end server loop */

	return;
}

/*
 * Check if FTS is active
 */
bool
FtsIsActive(void)
{
	return !skip_fts_probe;
}

/*
 * The purpose of this interface is to control whether to skip the FTS probe, currently only for the QE process.
 * At this stage, the QE node does not have an FTS process, and here is only the state on the QD under synchronization.
 */
void
SetSkipFtsProbe(bool skipFtsProbe)
{
	skip_fts_probe = skipFtsProbe;
}

static
void warp_call()
{
	cbdb_set_log_level(CBDB_LOG_DEBUG);
	switch (warp_call_i) 
	{
		case 1:
		{
			FILE * fd = NULL;
			char * buf = NULL;
			long bufsize;
			int rc;
				
			if (warp_local_fts_file == NULL || strlen(warp_local_fts_file) == 0)
			{
				printf("Invalid flag -L.");
				usage();
				goto warp_1_error;
			}
			
			fd = fopen(warp_local_fts_file, "r");
			if (!fd) 
			{
				printf("Can't open file: %s", warp_local_fts_file);
				goto warp_1_error;
			}

			if (fseek(fd, 0L, SEEK_END) == 0)
			{
				bufsize = ftell(fd);
				buf = palloc(sizeof(char) * (bufsize + 1));

				if (fseek(fd, 0L, SEEK_SET) != 0)
				{
					printf("Can't seek file: %s", warp_local_fts_file);
					goto warp_1_error;
				}

				size_t read_len = fread(buf, sizeof(char), bufsize, fd);
				if (ferror(fd) != 0)
				{
					printf("Can't read file: %s", warp_local_fts_file);
					goto warp_1_error;
				} 
				buf[read_len++] = '\0';
			}
			else
			{
				printf("Can't seek file: %s", warp_local_fts_file);
				goto warp_1_error;
			}

			rc = writeFTSDumpFromETCD(buf);
			if (rc != 0)
			{
				printf("Fail to write fts info into ETCD, rc=%d", rc);
				goto warp_1_error;
			}

			fclose(fd);
			pfree(buf);
			printf("Success dump FTS file into ETCD.");
			break;

warp_1_error:
			if (fd)
				fclose(fd);
			if (buf)
				pfree(buf);
			exit(-1);
		}
		case 2:
		{
			int rc;
			char * buf;

			rc = readFTSDumpFromETCD(&buf);
			if (rc != 0)
			{
				printf("Fail to read fts info from ETCD, rc=%d", rc);
				exit(-1);
			}
			
			printf("%s", buf);
			
			pfree(buf);
			break;
		}
		case 3:
		{
			int rc;
			char * buf;

			rc = delFTSInfoFromETCD();
			if (rc != 0)
			{
				printf("Fail to delete fts info from ETCD, rc=%d", rc);
				exit(-1);
			}

			rc = readFTSDumpFromETCD(&buf);
			if (rc == 0 || buf != NULL)
			{
				printf("Fail to delete fts info from ETCD, still can read it from ETCD.");
				pfree(buf);
				exit(-1);
			}

			printf("Done!");
			break;
		}
		case 4:
		{
			char * rewrite_info = "abc";
			char * rewrite_sql = NULL;
			PGresult   *res;
			char * connstr_source = "";

			PGconn *conn = PQconnectdb(connstr_source);

			if (PQstatus(conn) == CONNECTION_BAD)
			{
				printf("Error: %s", PQerrorMessage(conn));
				goto warp_4_error;
			}

			printf("connected to server");

			rewrite_sql = pg_malloc0(strlen(REWRITE_SEGMENTS_INFO_SQL) + strlen(rewrite_info) + 1);
			snprintf(rewrite_sql,strlen(REWRITE_SEGMENTS_INFO_SQL) + strlen(rewrite_info) + 1, REWRITE_SEGMENTS_INFO_SQL, rewrite_info);

			printf("executing %s", rewrite_sql);

			res = PQexec(conn, rewrite_sql);
			if (!res ||
				PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				cbdb_log_fatal("query failed: %s", PQerrorMessage(conn));
				goto warp_4_error;
			}

			PQclear(res);

			printf("Executed done.");

			pfree(rewrite_sql);
			PQfinish(conn);

			break;
warp_4_error:
			if (rewrite_sql)
			{
				pfree(rewrite_sql);
			}
			PQfinish(conn);
			exit(-1);
		}
		case 5:
		{
			int rc;
			char * buf;

			rc = readStandbyPromoteReadyFromETCD(&buf);
			if (rc != 0)
			{
				printf("Fail to read fts info from ETCD, rc=%d", rc);
				exit(-1);
			}
			
			printf("%s", buf);
			
			pfree(buf);
			break;
		}
		default:
		{
			printf("Invalid flag -W, Nothing to do.");
			usage();
			break;
		}
	}
}

static char *
parseUser(char * user)
{
	char * connstr_up = NULL;
	int length = 0;
	if (user && strlen(user))
	{
		length = strlen(CONNECTION_SOURCE_ONLY_USER) + strlen(user);
		connstr_up = palloc0(length);
		snprintf(connstr_up, length, CONNECTION_SOURCE_ONLY_USER, user);
	}
	return connstr_up;
}

int
main(int argc, char **argv)
{
	fts_config context_config = {
		.probe_retries = GP_FTS_PROBE_RETRIES,
		.probe_timeout = GP_FTS_PROBE_TIMEOUT,
		.probe_interval = GP_FTS_PROBE_INTERVAL,

		.connstr_source = NULL,
		.disable_promote_standby = false,
		.one_round = false
	};

	int c;
	char *user = NULL;
	char *etcd_conf_name = NULL;

	cbdb_set_log_level(CBDB_LOG_INFO);
	fts_directory_name = getCbdbLogPath();
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0
			|| strcmp(argv[1], "-h") == 0)
		{
			usage();
			exit(0);
		}
	}

	optind = 1;
	while (optind < argc)
	{
		while ((c = getopt_long(argc, argv, "F:u:d:n:W:H:P:L:U:aACDR:T:I:v",
								long_options, NULL)) != -1)
		{
			switch (c)
			{
				case 'F':
					{
						etcd_conf_name = pg_strdup(optarg);
						break;
					}
				case 'u':
					{
						char	   *_fts_ha_lock_lease;
						_fts_ha_lock_lease = pg_strdup(optarg);
						fts_ha_lock_lease_timeout= atoi(_fts_ha_lock_lease);
						pfree(_fts_ha_lock_lease);
						break;
					}
				case 'd':
					{
						fts_directory_name = pg_strdup(optarg);
						break;
					}
				case 'n':
					{
						char	   *_line_number;
						_line_number = pg_strdup(optarg);
						int line_number = atoi(_line_number);
						cbdb_set_max_log_file_line(line_number);
						pfree(_line_number);
						break;
					}
				case 'C':
					{
						fts_k8s_compatibility_enabled = true;
						break;
					}
				case 'a':
					{
						fts_standalone_enabled = true;
						break;
					}
				case 'W':
					{
						char	   *warp_w;
						warp_w = pg_strdup(optarg);
						warp_call_i = atoi(warp_w);
						free(warp_w);
						break;
					}
				case 'L':
					{
						warp_local_fts_file = pg_strdup(optarg);
						break;
					}
				case 'U':
					{
						user = pg_strdup(optarg);
						break;
					}
				case 'A':
					{
						context_config.one_round = true;
						break;
					}
				case 'D':
					{
						context_config.disable_promote_standby = true;
						break;
					}
				case 'R':
					{
						char	   *_probe_retries;
						_probe_retries = pg_strdup(optarg);
						context_config.probe_retries = atoi(_probe_retries);
						pfree(_probe_retries);
						break;
					}
				case 'T':
					{
						char	   *_probe_timeout;
						_probe_timeout = pg_strdup(optarg);
						context_config.probe_timeout = atoi(_probe_timeout);
						pfree(_probe_timeout);
						break;
					}
				case 'I':
					{
						char	   *_probe_interval;
						_probe_interval = pg_strdup(optarg);
						context_config.probe_interval = atoi(_probe_interval);
						pfree(_probe_interval);
						break;
					}
				case 'v':
					{
						cbdb_set_log_level(CBDB_LOG_DEBUG);
						break;
					}
				default:
					/* do nothing */ 
					break;
			}
		}
	}

	context_config.connstr_source = parseUser(user);
	pfree(user);

	if (!cbdb_set_log_file(fts_directory_name, FTS_LOG_FILE_NAME_DEFAULT))
	{
		usage();
		exit(-1);
	}

	if(!readGpEtcdConfigFromCbdbFile(etcd_conf_name))
	{
		cbdb_log_fatal("Read ETCD service configuration file failed.");
		usage();
		destoryETCD();
		exit(-1);
	}

	if(!initETCD(etcd_endpoints, etcd_namespace, etcd_account_id, etcd_cluster_id))
	{
		cbdb_log_fatal("Init ETCD service failed.");
		exit(-1);
	}

	if (warp_call_i != -1)
	{
		warp_call();
		destoryETCD();
		exit(0);
	}
	
	FtsLoop(&context_config);

	pfree(context_config.connstr_source);
	pfree(etcd_conf_name);
	pfree(fts_directory_name);

	cleanCdbComponentInfos();
	destoryETCD();
	return 0;
}

#else

int
main(int argc, char **argv)
{
	int c;

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0
			|| strcmp(argv[1], "-h") == 0)
		{
			usage();
			exit(0);
		}
	}

	optind = 1;
	while (optind < argc)
	{
		while ((c = getopt_long(argc, argv, "F:u:d:n:W:H:P:L:U:aACDR:T:I:v",
								long_options, NULL)) != -1)
		{
			switch (c)
			{
			case 'A':
			case 'W': {
				return 0;
			}
			default: {
				// do nothing
			}
			}
		}
	}

	while (true) {
		// loop here
		sleep(60);
	}
	return 0;
}
#endif
