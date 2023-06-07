
#include "postgres_fe.h"
#ifndef USE_INTERNAL_FTS
#include "fts_etcd.h"
#include "postmaster/fts_comm.h"
#include "common/etcdutils.h"
#include "fe_utils/log.h"
#include <stdio.h>
#include <unistd.h>

static etcdlib_t *etcdlib;
static char *fts_dump_file_key = NULL;
static etcdlib_endpoint_t fts_etcd_endpoints[GP_ETCD_ENDPOINTS_NUM] = {0};
static int fts_etcd_endpoints_num = 0;
static char *fts_standby_promote_ready_key=NULL;
extern const char *progname;

bool
initETCD(char *etcd_endpoints, const char *etcd_namespace, const char *etcd_account_id, const char *etcd_cluster_id)
{
    bool isEtcdKeyConfigured = false;
    etcdlib_endpoint_t *petcd_endpoints = fts_etcd_endpoints;
    fts_dump_file_key = (char *)palloc0(sizeof(char)*GP_ETCD_KEY_LEN);
    isEtcdKeyConfigured = generateGPSegConfigKey(fts_dump_file_key, etcd_namespace, etcd_account_id,
                                                  etcd_cluster_id, etcd_endpoints, petcd_endpoints, &fts_etcd_endpoints_num);
    Assert((isEtcdKeyConfigured) != false && (fts_dump_file_key[0] != '\0'));
    fts_standby_promote_ready_key = (char *)palloc0(sizeof(char)*GP_ETCD_KEY_LEN);
    generateGPFtsPromoteReadyKey(fts_standby_promote_ready_key, etcd_namespace, etcd_account_id, etcd_cluster_id);
    Assert(fts_standby_promote_ready_key[0] != '\0');
    cbdb_log_info("initETCD susscessfully with fts_dump_file_key:%s.", fts_dump_file_key);
    etcdlib = etcdlib_create(petcd_endpoints, fts_etcd_endpoints_num, 0);
    return etcdlib != NULL;
}

void
destoryETCD(void)
{
    etcdlib_destroy(etcdlib);
    if (fts_dump_file_key)
    {
        pfree(fts_dump_file_key);
        fts_dump_file_key = NULL;
    }
    for (int i = 0; i < fts_etcd_endpoints_num; i++)
    {
	if (fts_etcd_endpoints[i].etcd_host)
	{
		pfree(fts_etcd_endpoints[i].etcd_host);
		fts_etcd_endpoints[i].etcd_host = NULL;
	}
    }
}

int
readFTSDumpFromETCD(char ** value)
{
    return etcdlib_get(etcdlib, fts_dump_file_key, value, NULL);
}

int
writeFTSDumpFromETCD(char * value)
{
    return etcdlib_set(etcdlib, fts_dump_file_key, value, 0, false);
}

int
delFTSInfoFromETCD(void)
{
    return etcdlib_del(etcdlib, fts_dump_file_key);
}

int
renewFTSLeaseFromETCD(long long lease, int timeout)
{
    int res = 0;
    Assert(lease != 0);

    for (int i = 0; i <  timeout; i++)
    {
        res = etcdlib_renew_lease(etcdlib, lease);
        if (res == 0)
            break;
        else
            cbdb_log_fatal("renewFTSLeaseFromETCD failed and retry to renew lease.");

        sleep(1);
    }
    return res;
}

bool
getFTSLockFromETCD(const char *key, char **lock, long long *lease, int lease_timeout, char *hostname, char *hostnamekey)
{
    int rc;

    Assert((key != NULL) && (strcmp(key, "") != 0));
    Assert((hostname != NULL) && (strcmp(hostname, "") != 0));
    Assert(lease != NULL);

    rc = etcdlib_grant_lease(etcdlib, lease, lease_timeout);
    if (rc != 0)
    {
        cbdb_log_fatal("FTS getFTSLockFromETCD failed to generate lease for lock rc: %d.", rc);
        return false;
    }

    rc = etcdlib_lock(etcdlib, key, *lease, lock);
    if (rc != 0)
    {
        cbdb_log_warning("FTS getFTSLockFromETCD failed to get FTS lock rc: %d.", rc);
        return false;
    }

    if (*lock == NULL)
    {
        cbdb_log_fatal("FTS getFTSLockFromETCD could not retrieve FTS lock and wating as standby node.");
        return false;
    }
    else
    {
	rc = etcdlib_set(etcdlib, hostnamekey, hostname, 0, false);
	if (rc != 0)
	{
             cbdb_log_fatal("FTS getFTSLockFromETCD failed to set hostname key for node: %s.", hostname);
             return false;
	}
	cbdb_log_info("FTS getFTSLockFromETCD successfully retrieve FTS lock: %s, lease: %lld, %s to be promoted as primary work node.", *lock, *lease, hostname);
    }

    return true;
}

int
readStandbyPromoteReadyFromETCD(char ** value)
{
    return etcdlib_get(etcdlib, fts_standby_promote_ready_key, value, NULL);
}

#endif