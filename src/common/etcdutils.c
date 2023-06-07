/*-------------------------------------------------------------------------
 *
 * etcdutils.c
 *	  A simple lib for etcd config file parsing
 *
 *
 * Portions Copyright (c) 2022-2023, Cloudberry Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "common/etcdutils.h"

/*
 * split: function used to split a string with special separator.
 * parameter src OUT
 * parameter separator IN
 * parameter dest OUT
 * parameter num OUT
 * return ture if configuration parse succeeded, otherwise retrun false.
 */
static bool
split(char *src, const char *separator, char **dest, int *num)
{
    char *pNext;
    int count = 0;
    if (src == NULL || src[0] == '\0')
        return false;
    if (separator == NULL || separator[0] == '\0')
        return false;
    pNext = (char *)strtok(src,separator);
    while (pNext != NULL)
    {
        *dest++ = pNext;
        ++count;
        pNext = (char *)strtok(NULL,separator);
    }  
    *num = count;
    return true;
}

/*
 * parseEtcdHostConfiguration: function used to parse etcdhost and etcdport inforamtion from a ETCD configure parameter.
 * parameter gp_etcd_endpoint IN
 * parameter gp_etcd_host OUT
 * parameter gp_etcd_port OUT
 * return ture if configuration parse succeeded, otherwise retrun false.
 */
static bool
parseEtcdHostConfiguration(const char *gp_etcd_endpoint, char *gp_etcd_host, int *gp_etcd_port)
{
    char etcd_host[GP_ETCD_HOSTNAME_LEN];
    char etcd_port[GP_ETCD_PORT_LEN];
    memset(etcd_host, 0, GP_ETCD_HOSTNAME_LEN);
    memset(etcd_port, 0, GP_ETCD_PORT_LEN);

    Assert(gp_etcd_endpoint != NULL && (strcmp(gp_etcd_endpoint, "") != 0));

    if (sscanf(gp_etcd_endpoint, "%[^':']:%s", etcd_host, etcd_port) == GPETCDCONFIGNUMATTR)
    {
        if (gp_etcd_host != NULL)
        {
            if (strlen(etcd_host) >= GP_ETCD_HOSTNAME_LEN-1)
            	return false;
            strncpy(gp_etcd_host, etcd_host, GP_ETCD_HOSTNAME_LEN);
        }
        if (gp_etcd_port != NULL)
        {
            *gp_etcd_port = atoi(etcd_port);
        }
     }
     else
	return false;

    return true;
}

/*
 * generateGPSegConfigKey: function used to generate ETCD configuration key and endpoint information.
 * parameter fts_dump_file_key OUT
 * parameter gp_etcd_namespace IN
 * parameter gp_etcd_account_id IN
 * parameter gp_etcd_cluster_id IN
 * parameter gp_etcd_endpoints OUT
 * parameter etcd_endpoints OUT
 * parameter etcd_endpoints_num OUT
 * return ture if configuration parse succeeded, otherwise retrun false.
 */
bool
generateGPSegConfigKey(char *fts_dump_file_key, const char *gp_etcd_namespace, const char *gp_etcd_account_id,
						const char *gp_etcd_cluster_id, char *gp_etcd_endpoints, etcdlib_endpoint_t *etcd_endpoints,
						int *etcd_endpoints_num)
{
    char *endpoints_list[GP_ETCD_ENDPOINTS_NUM] = {0};
    etcdlib_endpoint_t *petcd_endpoints = etcd_endpoints;
    bool ret = true;
    int fts_dump_file_key_length = 0;
    int etcd_port = 0;
    char etcd_host[GP_ETCD_HOSTNAME_LEN];
    memset(etcd_host, 0, GP_ETCD_HOSTNAME_LEN);

    Assert(fts_dump_file_key != NULL);
    Assert(gp_etcd_namespace != NULL && (strcmp(gp_etcd_namespace, "") != 0));
    Assert(gp_etcd_account_id != NULL && (strcmp(gp_etcd_account_id, "") != 0));
    Assert(gp_etcd_cluster_id != NULL && (strcmp(gp_etcd_cluster_id, "") != 0));
    Assert(gp_etcd_endpoints != NULL && (strcmp(gp_etcd_endpoints, "") != 0));

    fts_dump_file_key_length = snprintf(fts_dump_file_key, GP_ETCD_KEY_LEN, "%s/%s/%s/%s/%s", \
             FTS_METADATA_DIR_PREFIX, gp_etcd_namespace, gp_etcd_account_id, gp_etcd_cluster_id, FTS_DUMP_FILE_KEY);

    if (fts_dump_file_key_length == 0 || fts_dump_file_key_length > GP_ETCD_KEY_LEN)
        return false;

    if (fts_dump_file_key == NULL || fts_dump_file_key[0] == '\0')
        return false;

    ret = split(gp_etcd_endpoints, GP_ETCD_SEPERATOR, endpoints_list, etcd_endpoints_num);
    if (!ret)
        return false;

    if (etcd_endpoints_num == 0 || *etcd_endpoints_num > GP_ETCD_ENDPOINTS_NUM)
        return false;

    for (int i = 0; i < (*etcd_endpoints_num); i++, petcd_endpoints++)
    {
        ret = parseEtcdHostConfiguration(endpoints_list[i], etcd_host, &etcd_port);
        if (!ret || etcd_host[0] == '\0' || etcd_port == 0 )
            return false;
        petcd_endpoints->etcd_host = pstrdup(etcd_host);
        petcd_endpoints->etcd_port = etcd_port;
        etcd_port = 0;
    }

    return true;
}

/*
 * generateGPFtsPromoteReadyKey: function used to generate FTS promote ready key.
 * parameter fts_standby_promote_ready_key OUT
 * parameter gp_etcd_namespace IN
 * parameter gp_etcd_account_id IN
 * parameter gp_etcd_cluster_id IN
 * return ture if configuration parse succeeded, otherwise retrun false.
 */
void
generateGPFtsPromoteReadyKey(char *fts_standby_promote_ready_key, const char *gp_etcd_namespace, const char *gp_etcd_account_id, const char *gp_etcd_cluster_id)
{
    Assert(fts_standby_promote_ready_key != NULL);
    Assert((gp_etcd_namespace != NULL) && (strcmp(gp_etcd_namespace, "") != 0));
    Assert(gp_etcd_account_id != NULL && (strcmp(gp_etcd_account_id, "") != 0));
    Assert(gp_etcd_cluster_id != NULL && (strcmp(gp_etcd_cluster_id, "") != 0));

    snprintf(fts_standby_promote_ready_key, GP_ETCD_KEY_LEN, "%s/%s/%s/%s/%s",
             FTS_METADATA_DIR_PREFIX, gp_etcd_namespace, gp_etcd_account_id, gp_etcd_cluster_id, FTS_STANDBY_PROMOTE_READY_KEY);
}
