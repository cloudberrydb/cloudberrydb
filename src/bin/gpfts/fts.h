/*-------------------------------------------------------------------------
 *
 * fts.h
 *	  Interface for fault tolerance service (FTS).
 *
 * IDENTIFICATION
 *		src/bin/gpfts/fts.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef USE_INTERNAL_FTS
#ifndef FTS_H
#define FTS_H

#include "nodes/pg_list.h"
#include "postmaster/fts_comm.h"

#define PRIMARY_CONFIG(fts_info) \
	fts_info->primary_cdbinfo->config

#define MIRROR_CONFIG(fts_info) \
	fts_info->mirror_cdbinfo->config

#define POSTMASTER_IN_STARTUP_MSG "the database system is starting up"
#define POSTMASTER_IN_RECOVERY_MSG "the database system is in recovery mode"
#define POSTMASTER_AFTER_PROMOTE_STANDBY_IN_RECOVERY_DETAIL_MSG "waiting for distributed transaction recovery to complete"
#define POSTMASTER_IN_RECOVERY_DETAIL_MSG "last replayed record at"

#define GP_FTS_PROBE_RETRIES 5
#define GP_FTS_PROBE_TIMEOUT 20
#define GP_FTS_PROBE_INTERVAL 60
#define FTS_ETCD_CONF_FILE_NAME "bin/config/cbdb_etcd_default.conf"
#define FTS_GPHOME_PATH "GPHOME"
#define FTS_CBDB_PATH "COORDINATOR_DATA_DIRECTORY"
#define FTS_CBDB_PATH_LEN 256
#define FTS_ETCD_HA_LOCK_KEY "fts_ha_lock"
#define FTS_ETCD_HA_HOSTNAME_KEY "fts_ha_hostname"
#define FTS_HOSTNAME_LEN 128
#define FTS_ETCD_PATH_KEY_LEN 256
#define FTS_ETCD_LOCK_RETRY_WAIT 10
#define FTS_ETCD_LEASE_PROBE 8
#define FTS_DNS_WAIT_TIMEOUT 60

typedef struct {
	int probe_retries;
	int probe_timeout;
	int probe_interval;

	char *connstr_source;
	bool disable_promote_standby;
	bool one_round;
} fts_config;

extern bool FtsIsActive(void);
extern void SetSkipFtsProbe(bool skipFtsProbe);
void FtsProbeMain(int argc, char **argv);

CdbComponentDatabases * getCdbComponents();
void initCdbComponentDatabases(GpSegConfigEntry * configs, int total_dbs);

#define REWRITE_SEGMENTS_INFO_SQL \
	"SELECT pg_catalog.gp_rewrite_segments_info('%s');"

#define CONNECTION_SOURCE_ONLY_USER "%s"
#define CONNECTION_SOURCE_TEMPLATE_WITH_UP "postgresql://%s@%s:%d/postgres"
#define CONNECTION_SOURCE_TEMPLATE "postgresql://%s:%d/postgres"

#define FAILOVER_SEGMENT_CHECK_BACK_PATH "failover/segment_check_back.sh"
#define FAILOVER_SEGMENT_ALL_DOWN_PATH "failover/segment_all_down.sh"
#define FAILOVER_MASTER_CHECK_BACK_PATH "failover/master_check_back.sh"

#define FTS_LOG_FILE_DIRECTORY_DEFAULT "log/fts"
#define FTS_LOG_FILE_DIRECTORY_TMP "/tmp/fts"
#define FTS_LOG_FILE_NAME_DEFAULT "fts"
#define FTS_MAX_DIRECTORY_NAME 200

extern char *bindir;
extern bool disable_auto_failover;
extern bool immediately_auto_failover;
#endif   /* FTS_H */
#endif