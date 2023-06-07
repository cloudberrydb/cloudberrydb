/*-------------------------------------------------------------------------
 *
 * gp_fts_test.c
 *	  unit test for separated fts damone testing
 *
 *
 * Portions Copyright (c) 2022-2023, Cloudberry Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#ifndef USE_INTERNAL_FTS
#include "utils/etcd.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/memutils.h"
#include "cmockery.h"
#include "fts_etcd.h"
#include "fe_utils/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <uuid/uuid.h>

#define SIMPLELOCK "ftslock"
#define SIMPLEHOSTKEY "ftshost"
#define TEST_HOSTNAME "hashdata-fts-0"
#define TEST_NAMESPACE "default"
#define UUID_LEN 37
#define ETCD_LOCK_LEN 256

static char test_single_endpoints[] = GP_ETCD_TEST_SINGLE_ENDPOINTS;
static char test_multi_endpoints[] = GP_ETCD_TEST_MULTI_ENDPOINTS;
static char test_failover_endpoints[] = GP_ETCD_TEST_FAILOVER_ENDPOINTS;
const char *progname = NULL;

static void
FTSGetLockTest(char *lock_name) {
	char *fts_hostname = TEST_HOSTNAME;
	char *lock = NULL;
	long long lease = 0;
	bool res = getFTSLockFromETCD(lock_name, &lock, &lease, FTS_HA_LOCK_LEASE_TIMEOUT_DEFAULT, fts_hostname, SIMPLEHOSTKEY);
	assert_true(res);
	assert_int_not_equal(lease, 0);
}

static void
FTSRenewLockLeaseTest(char *lock_name) {
	char *fts_hostname = TEST_HOSTNAME;
	char *lock = NULL;
	long long lease = 0;
	bool res = getFTSLockFromETCD(lock_name, &lock, &lease, FTS_HA_LOCK_LEASE_TIMEOUT_DEFAULT, fts_hostname, SIMPLEHOSTKEY);
	assert_true(res);
	assert_int_not_equal(lease, 0);
	int ret = renewFTSLeaseFromETCD(lease, FTS_HA_LOCK_LEASE_TIMEOUT_DEFAULT);
	assert_int_equal(ret, 0);
}

static void
test_fts_lock_function(void **state)
{
	char lock_name[ETCD_LOCK_LEN];
	memset(lock_name, 0, ETCD_LOCK_LEN);

	uuid_t uid_account;
	char uuid_account[UUID_LEN];
	uuid_generate(uid_account);
	uuid_unparse(uid_account, uuid_account);

	uuid_t uid_cluster;
	char uuid_cluster[UUID_LEN];
	uuid_generate(uid_cluster);
	uuid_unparse(uid_cluster, uuid_cluster);

	snprintf(lock_name, sizeof(lock_name), "/%s/%s/%s", uuid_account, uuid_cluster, SIMPLELOCK);

	bool res = initETCD(test_single_endpoints, TEST_NAMESPACE, uuid_account, uuid_cluster);
	assert_true(res);
	FTSGetLockTest(lock_name);
	destoryETCD();
}

static void
test_fts_lock_multi_function(void **state)
{
	char lock_name[ETCD_LOCK_LEN];
	memset(lock_name, 0, ETCD_LOCK_LEN);

	uuid_t uid_account;
	char uuid_account[UUID_LEN];
	uuid_generate(uid_account);
	uuid_unparse(uid_account, uuid_account);

	uuid_t uid_cluster;
	char uuid_cluster[UUID_LEN];
	uuid_generate(uid_cluster);
	uuid_unparse(uid_cluster, uuid_cluster);

	snprintf(lock_name, sizeof(lock_name), "/%s/%s/%s", uuid_account, uuid_cluster, SIMPLELOCK);

	bool res = initETCD(test_multi_endpoints, TEST_NAMESPACE, uuid_account, uuid_cluster);
	assert_true(res);
	FTSGetLockTest(lock_name);
	destoryETCD();
}


static void
test_fts_lock_function_failover_endpoints(void **state)
{
	char lock_name[ETCD_LOCK_LEN];
	memset(lock_name, 0, ETCD_LOCK_LEN);

	uuid_t uid_account;
	char uuid_account[UUID_LEN];
	uuid_generate(uid_account);
	uuid_unparse(uid_account, uuid_account);

	uuid_t uid_cluster;
	char uuid_cluster[UUID_LEN];
	uuid_generate(uid_cluster);
	uuid_unparse(uid_cluster, uuid_cluster);

	snprintf(lock_name, sizeof(lock_name), "/%s/%s/%s", uuid_account, uuid_cluster, SIMPLELOCK);

	bool res = initETCD(test_failover_endpoints, TEST_NAMESPACE, uuid_account, uuid_cluster);
	assert_true(res);
	FTSGetLockTest(lock_name);
	destoryETCD();
}

static void
test_fts_lock_lease_function(void **state)
{
	char lock_name[ETCD_LOCK_LEN];
	memset(lock_name, 0, ETCD_LOCK_LEN);

	uuid_t uid_account;
	char uuid_account[UUID_LEN];
	uuid_generate(uid_account);
	uuid_unparse(uid_account, uuid_account);

	uuid_t uid_cluster;
	char uuid_cluster[UUID_LEN];
	uuid_generate(uid_cluster);
	uuid_unparse(uid_cluster, uuid_cluster);

	snprintf(lock_name, sizeof(lock_name), "/%s/%s/%s", uuid_account, uuid_cluster, SIMPLELOCK);

	bool res = initETCD(test_single_endpoints, TEST_NAMESPACE, uuid_account, uuid_cluster);
	assert_true(res);
	FTSRenewLockLeaseTest(lock_name);
	
	destoryETCD();
}

int main (int argc, char* argv[]) {
	progname = get_progname(argv[0]);

	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_fts_lock_function),
		unit_test(test_fts_lock_multi_function),
		unit_test(test_fts_lock_function_failover_endpoints),
		unit_test(test_fts_lock_lease_function)
	};
	return run_tests(tests);
	return 0;
}
#else 
int
main(int argc, char* argv[])
{
    return 0;
}
#endif
