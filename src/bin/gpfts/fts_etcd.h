#ifndef USE_INTERNAL_FTS

#ifndef FTS_ETCD_H
#define FTS_ETCD_H

#include "postgres_fe.h"
#include "utils/etcd.h"
#include <stddef.h>

#define FTS_HA_DEFAULT_TTL 10
#define FTS_HA_STARTUP_TTL 30
#define FTS_HA_BACKOFF_TIMEOUT 30
#define FTS_KEY_DEFAULT_TIMEOUT 60*60*24*100
#define FTS_HA_LOCK_LEASE_TIMEOUT_DEFAULT 30

extern bool initETCD(char *etcd_endpoints, const char *etcd_namespace, const char *etcd_account_id, const char *etcd_cluster_id);
extern void destoryETCD(void);
extern int readFTSDumpFromETCD(char ** value);
extern int writeFTSDumpFromETCD(char * value);
extern int delFTSInfoFromETCD(void);
extern int readStandbyPromoteReadyFromETCD(char ** value);
extern int renewFTSLeaseFromETCD(long long lease, int timeout);
extern bool getFTSLockFromETCD(const char *key, char **lock, long long *lease, int lease_timeout, char *hostnamem, char *hostnamekey);
#endif
#endif