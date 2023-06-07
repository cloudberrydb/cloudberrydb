/**
 * Test program for testing the etcdlib.
 * Prerequisite is that etcdlib is started on localhost on default port (2379)
 * tested with etcd 2.3.7
 */


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <uuid/uuid.h>

#include "postgres.h"
#include "cmockery.h"
#include "utils/etcd.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/memutils.h"
#include "common/etcdutils.h"

#ifndef USE_INTERNAL_FTS

static etcdlib_t *etcdlib;
static MemoryContext testMemoryContext = NULL;
static pthread_mutex_t lease_lock = PTHREAD_MUTEX_INITIALIZER;

#define SIMPLEKEY "simplekey"
#define SIMPLEVALUE "testvalue"
#define DIRECTORYKEY "/cbdb/fts/default/e3cb5400-9589-918d-c178-82d500deac6e/7bc05356-67f9-49fe-804e-12fe30b093ef/fts_dump_file_key"
#define SIMPLELOCK "simplelock"
#define LEASETIMEOUT 12
#define ETCD_TEST_HOST "192.168.180.86"
#define ETCD_TEST_PORT 2379
#define ETCD_TEST_SINGLE_ENDPOINTS_NUM 1
#define ETCD_TEST_MULTI_ENDPOINTS_NUM 3
#define UUID_LEN 37
#define ETCD_LOCK_LEN 256
#define ETCD_LOCK_KEY "fts_ha_lock"
#define METADATA_DIR_PREFIX "/cbdb/fts"
#define NAMESPACE "default"

/*
 *  * Mock PG_RE_THROW, because we are not using real elog.o.
 *   * The closest mockery is to call siglongjmp().
 *    */
#undef PG_RE_THROW
#define PG_RE_THROW() siglongjmp(*PG_exception_stack, 1)

static char test_endpoint_list[ETCD_TEST_MULTI_ENDPOINTS_NUM][GP_ETCD_HOSTNAME_LEN] = {"192.168.180.86", "192.168.180.87", "192.168.180.88"};
static char test_failover_endpoint_list[ETCD_TEST_MULTI_ENDPOINTS_NUM+1][GP_ETCD_HOSTNAME_LEN] = {"127.0.0.1", "192.168.180.86", "192.168.180.87", "192.168.180.88"};

static void
_errfinish_impl()
{
        PG_RE_THROW();
}

#define EXPECT_EREPORT(LOG_LEVEL)     \
        if (LOG_LEVEL < ERROR )\
        { \
                expect_value(errstart, elevel, (LOG_LEVEL)); \
                expect_any(errstart, domain); \
                will_return(errstart, false); \
        } \
        else \
        { \
                expect_value(errstart_cold, elevel, (LOG_LEVEL)); \
                expect_any(errstart_cold, domain); \
                will_return_with_sideeffect(errstart_cold, false, &_errfinish_impl, NULL); \
        } \

static etcdlib_endpoint_t etcd_endpoints[GP_ETCD_ENDPOINTS_NUM] = {0};
static etcdlib_endpoint_t etcd_multi_endpoints[GP_ETCD_ENDPOINTS_NUM] = {0};
static etcdlib_endpoint_t etcd_failover_endpoints[GP_ETCD_ENDPOINTS_NUM] = {0};
static int etcd_endpoints_num = 0;
static char *fts_dump_file_key = NULL;

static char*
generateLockName() {
	uuid_t uid_account;
	char uuid_account[UUID_LEN];
	char lock_name[ETCD_LOCK_LEN];
	memset(lock_name, 0, ETCD_LOCK_LEN);
	uuid_generate(uid_account);
	uuid_unparse(uid_account, uuid_account);

	uuid_t uid_cluster;
	char uuid_cluster[UUID_LEN];
	uuid_generate(uid_cluster);
	uuid_unparse(uid_cluster, uuid_cluster);

	memset(lock_name, 0, sizeof(char)*ETCD_LOCK_LEN);
	snprintf(lock_name, ETCD_LOCK_LEN, "%s/%s/%s/%s/%s",
		METADATA_DIR_PREFIX, NAMESPACE, uuid_account, uuid_cluster, ETCD_LOCK_KEY);

	printf("generateLockName lock key: %s\n", lock_name);
	return strdup(lock_name);
}

static void
simpleWriteTest() {
	int res = 0;
	char*value = NULL;

	res = etcdlib_set(etcdlib, SIMPLEKEY, SIMPLEVALUE, 0, false);
	assert_int_equal(res, 0);

	res = etcdlib_get(etcdlib, SIMPLEKEY, &value, NULL);
	assert_int_equal(res, 0);
	assert_true(value != NULL);
	assert_string_equal(value, SIMPLEVALUE);

	res = etcdlib_del(etcdlib, SIMPLEKEY);
	assert_int_equal(res, 0);

	// should not get
	res = etcdlib_get(etcdlib, SIMPLEKEY, &value, NULL);
	assert_int_not_equal(res, 0);

	if (value)
		pfree(value);
}

static void
directroyWriteTest() {
	int res = 0;
	char*value = NULL;

	res = etcdlib_set(etcdlib, DIRECTORYKEY, SIMPLEVALUE, 0, false);
	assert_int_equal(res, 0);

	res = etcdlib_get(etcdlib, DIRECTORYKEY, &value, NULL);
	assert_int_equal(res, 0);
	assert_true(value != NULL);
	assert_string_equal(value, SIMPLEVALUE);

	res = etcdlib_del(etcdlib, DIRECTORYKEY);
	assert_int_equal(res, 0);

	/* should not get */
	res = etcdlib_get(etcdlib, DIRECTORYKEY, &value, NULL);
	assert_int_not_equal(res, 0);

	if (value)
		pfree(value);
}

int
simpleLeaseTest() {
 	int res = 0;
 	long long lease = 0;
 	char*value = NULL;

 	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
        assert_int_not_equal(lease, 0);

 	res = etcdlib_set(etcdlib, SIMPLEKEY, SIMPLEVALUE, lease, false);
	assert_int_equal(res, 0);

 	res = etcdlib_get(etcdlib, SIMPLEKEY, &value, NULL);
	assert_int_equal(res, 0);

 	if (value && strcmp(value, SIMPLEVALUE)) {
 		printf("etcdlib test error: expected testvalue got %s\n", value);
		assert_true(false);
 	}

	sleep(LEASETIMEOUT + 5);

 	res = etcdlib_get(etcdlib, SIMPLEKEY, &value, NULL);
	assert_int_not_equal(res, 0);

 	if (value)
 	    pfree(value);
 	return res;
 }

int
simpleLockTest() {
	int res = 0;
	char*lock_key = NULL;
	long long lease = 0;

	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, SIMPLELOCK, lease, &lock_key);
	assert_int_equal(res, 0);

	sleep(LEASETIMEOUT/2);

	res = etcdlib_unlock(etcdlib, lock_key);
	assert_int_equal(res, 0);

	if (lock_key)
		pfree(lock_key);
	return res;
}

int
simpleCheckLeaderTest() {
        int res = 0;
        bool isLeader = false;
        bool failover_result = false;

        res = etcdlib_get_leader(etcdlib, &isLeader);
        assert_int_equal(res, 0);

        if (isLeader) {
                printf("simpleCheckLeaderTest already found leader node.\n");
                assert_int_equal(isLeader, true);
        } else {
                printf("simpleCheckLeaderTest already not found leader node, try failover.\n");
                failover_result = test_etcd_failover(&etcdlib);
                assert_true(failover_result);
        }

        return res;
}

int
metaLockTest() {
	int res = 0;
	char* lock_key = NULL;
	long long lease = 0;
	char *lock_name = generateLockName();

	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	assert_int_equal(res, 0);

	sleep(LEASETIMEOUT/2);

	res = etcdlib_unlock(etcdlib, lock_key);
	assert_int_equal(res, 0);

	if (lock_key)
		pfree(lock_key);
	return res;
}

int
metaTimeoutLockTest() {
	int res = 0;
	char* lock_key = NULL;
	long long lease = 0;
	char *lock_name = generateLockName();
	
	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	assert_int_equal(res, 0);

	sleep(LEASETIMEOUT*2);

 	lease = 0;
	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	assert_int_equal(res, 0);

	if (lock_key)
		pfree(lock_key);
	return res;
}

static void*
etcdLockRenewLease(char *lock_name) {
	int rc = 0;
	int rc_lock = 0;
	long long lease = 0;
	int res;
	char* lock_key = NULL;

	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	assert_int_equal(res, 0);

	printf("etcdLockRenewLease successfully retrived lock.\n");

	int loop = 0;
	while(loop <= 2*LEASETIMEOUT) {
		pthread_mutex_lock(&lease_lock);
		res = etcdlib_renew_lease(etcdlib, lease);
		pthread_mutex_unlock(&lease_lock);
		assert_int_equal(res, 0);
		loop++;
		sleep(1);
	}

	res = etcdlib_unlock(etcdlib, lock_key);
	assert_int_equal(res, 0);

	if (lock_key)
		pfree(lock_key);
}

static void*
etcdLockRenewTryLock(char *lock_name) {
	int rc = 0;
	int rc_lock = 0;
	long long lease = 0;
	int res;
	char* lock_key = NULL;

	sleep(2);

	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	pthread_mutex_lock(&lease_lock);
	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	pthread_mutex_unlock(&lease_lock);
	assert_int_not_equal(res, 0);

	printf("etcdLockRenewTryLock try lock failed as expected.\n");

	sleep(LEASETIMEOUT + 5);

	lease = 0;
	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	pthread_mutex_lock(&lease_lock);
	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	pthread_mutex_unlock(&lease_lock);
	assert_int_not_equal(res, 0);

	printf("etcdLockRenewTryLock try lock failed again as expected.\n");

	if (lock_key)
		pfree(lock_key);

}

int
metaRenewLockTest() {
	char *lock_name = generateLockName();
	pthread_t thread1;
	pthread_t thread2;
	int status = pthread_create(&thread1, NULL, etcdLockRenewLease, (void *)lock_name);
	status = pthread_create(&thread2, NULL, etcdLockRenewTryLock, (void *)lock_name);
	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);
}

static void*
etcdLockFirst(char *lock_name) {
	int rc = 0;
	int rc_lock = 0;
	long long lease = 0;
	int res;
	char* lock_key = NULL;

	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT*600);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	assert_int_equal(res, 0);

	printf("etcdLockFirst successfully retrived lock.\n");
	sleep(LEASETIMEOUT);

	res = etcdlib_unlock(etcdlib, lock_key);
	assert_int_equal(res, 0);

	if (lock_key)
		pfree(lock_key);
}

static void*
etcdLockFirstTimeout(char *lock_name) {
	int rc = 0;
	int rc_lock = 0;
	long long lease = 0;
	int res;
	char* lock_key = NULL;

	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	assert_int_equal(res, 0);

	if (lock_key)
		pfree(lock_key);
}

static void*
etcdLockSecond(char *lock_name) {
	int rc = 0;
	int rc_lock = 0;
	long long lease = 0;
	int res;
	char* lock_key = NULL;

	sleep(2);

	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	assert_int_not_equal(res, 0);

	printf("etcdLockSecond  try lock failed as expected.\n");

	sleep(LEASETIMEOUT + 5);

	lease = 0;
	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);
	assert_int_equal(res, 0);
	assert_int_not_equal(lease, 0);

	res = etcdlib_lock(etcdlib, lock_name, lease, &lock_key);
	assert_int_equal(res, 0);

	printf("etcdLockSecond successfully retrived lock.\n");
	sleep(1);

	res = etcdlib_unlock(etcdlib, lock_key);
	assert_int_equal(res, 0);

	if (lock_key)
		pfree(lock_key);
}

void
metaConcurrentLockTest() {
	int status = 0;
	char *lock_name = generateLockName();
	pthread_t thread1;
	pthread_t thread2;
	status = pthread_create(&thread1, NULL, etcdLockFirst, (void *)lock_name);
	status = pthread_create(&thread2, NULL, etcdLockSecond, (void *)lock_name);
	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);
}

void
metaConcurrentLockTimeoutTest() {
	int status = 0;
	char *lock_name = generateLockName();
	pthread_t thread1;
	pthread_t thread2;
	status = pthread_create(&thread1, NULL, etcdLockFirstTimeout, (void *)lock_name);
	status = pthread_create(&thread2, NULL, etcdLockSecond, (void *)lock_name);
	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);
}

/* temporarilyt disable this case cause the etcdlib watch functionality is currently used, should be added back once necessarily.
void* waitForChange(void*arg) {
	int *idx = (int*)arg;
	char *action = NULL;
	char *prevValue = NULL;
	char *value = NULL;
	char *rkey = NULL;
	long long modifiedIndex;

	printf("Watching for index %d\n", *idx);

	if(etcdlib_watch(etcdlib, "hier/ar", *idx, &action, &prevValue, &value, &rkey, &modifiedIndex) == 0){
		printf(" New value from watch : [%s]%s => %s\n", rkey, prevValue, value);
		if(action != NULL) free(action);
		if(prevValue != NULL) free(prevValue);
		if(rkey != NULL) free(rkey);
		if(value != NULL) free(value);
	}

	*idx = modifiedIndex+1;

	action = NULL;
	prevValue = NULL;
	value = NULL;
	rkey = NULL;

	if(etcdlib_watch(etcdlib, "hier/ar", *idx, &action, &prevValue, &value, &rkey, &modifiedIndex) == 0){
		printf(" New value from watch : [%s]%s => %s\n", rkey, prevValue, value);
		if(action != NULL) free(action);
		if(prevValue != NULL) free(prevValue);
		if(rkey != NULL) free(rkey);
	}

	return value;
}

int waitforchangetest() {
	int res = 0;
	char*value = NULL;

	etcdlib_set(etcdlib, "hier/ar/chi/cal", "testvalue1", 5, false);

	int index;
	etcdlib_get(etcdlib, "hier/ar/chi/cal", &value, &index);
	free(value);
	pthread_t waitThread;
	index++;
	pthread_create(&waitThread, NULL, waitForChange, &index);
	sleep(1);
	etcdlib_set(etcdlib, "hier/ar/chi/cal", "testvalue2", 5, false);
	sleep(1);
	etcdlib_set(etcdlib, "hier/ar/chi/cal", "testvalue3", 5, false);
	void *resVal = NULL;
	printf("joining\n");
	pthread_join(waitThread, &resVal);
	if(resVal == NULL || strcmp((char*)resVal,"testvalue3" ) != 0) {
		printf("etcdtest::waitforchange1 expected 'testvalue3', got '%s'\n", (char*)resVal);
		res = -1;
	}
	free(resVal);
	return res;
}
*/

static void
test_init_single_endpoints() {
	etcd_endpoints[0].etcd_host = (char *)malloc(sizeof(char)*GP_ETCD_HOSTNAME_LEN);
	strncpy(etcd_endpoints[0].etcd_host, ETCD_TEST_HOST, GP_ETCD_HOSTNAME_LEN);
	etcd_endpoints[0].etcd_port = ETCD_TEST_PORT;
}

static void
test_init_multi_endpoints() {
	for (int i = 0; i < ETCD_TEST_MULTI_ENDPOINTS_NUM; i++) {
		etcd_multi_endpoints[i].etcd_host = (char *)malloc(sizeof(char)*GP_ETCD_HOSTNAME_LEN);
		strncpy(etcd_multi_endpoints[i].etcd_host, test_endpoint_list[i], GP_ETCD_HOSTNAME_LEN);
		etcd_multi_endpoints[i].etcd_port = ETCD_TEST_PORT;
	}
}

static void
test_init_failover_endpoints() {
	for (int i = 0; i < ETCD_TEST_MULTI_ENDPOINTS_NUM+1; i++) {
		etcd_failover_endpoints[i].etcd_host = (char *)malloc(sizeof(char)*GP_ETCD_HOSTNAME_LEN);
		strncpy(etcd_failover_endpoints[i].etcd_host, test_failover_endpoint_list[i], GP_ETCD_HOSTNAME_LEN);
		etcd_failover_endpoints[i].etcd_port = ETCD_TEST_PORT;
	}
}

static void
test_init_endpoints() {
	test_init_single_endpoints();
	test_init_multi_endpoints();
	test_init_failover_endpoints();
}


static void 
test_etcd_write_test_function(void **state) 
{
	etcdlib_endpoint_t *petcd_endpoints = etcd_endpoints;
	etcdlib = etcdlib_create(petcd_endpoints, ETCD_TEST_SINGLE_ENDPOINTS_NUM, 0);
	simpleWriteTest();
	directroyWriteTest();
	simpleLeaseTest();
	etcdlib_destroy(etcdlib);
}

static void 
test_etcd_lock_test_function(void **state) 
{
	etcdlib_endpoint_t *petcd_endpoints = etcd_endpoints;
	etcdlib = etcdlib_create(petcd_endpoints, ETCD_TEST_SINGLE_ENDPOINTS_NUM, 0);
	simpleLockTest();
	metaLockTest();
	metaTimeoutLockTest();
	metaRenewLockTest();
	metaConcurrentLockTest();
	metaConcurrentLockTimeoutTest();
	etcdlib_destroy(etcdlib);
}

static void 
test_etcd_simple_failover_test_function(void **state)
{
	etcdlib_endpoint_t *petcd_endpoints = etcd_multi_endpoints;
	etcdlib = etcdlib_create(petcd_endpoints, ETCD_TEST_MULTI_ENDPOINTS_NUM, 0);
	simpleCheckLeaderTest();
	etcdlib_destroy(etcdlib);
}

static void
test_etcd_failure_failover_test_function(void **state)
{
	int res = 0;
	long long lease = 0;
	char*value = NULL;
	etcdlib_endpoint_t *petcd_endpoints = etcd_failover_endpoints;
	etcdlib = etcdlib_create(petcd_endpoints, ETCD_TEST_MULTI_ENDPOINTS_NUM+1, 0);
	
	res = etcdlib_grant_lease(etcdlib, &lease, LEASETIMEOUT);

	assert_int_equal(res, 0);
	etcdlib_destroy(etcdlib);
}

static void
SetupDataStructures(void **state)
{
	if (NULL == TopMemoryContext)
	{
		assert_true(NULL == testMemoryContext);
		MemoryContextInit();

		testMemoryContext = AllocSetContextCreate(TopMemoryContext,
        	                      "Test Context",
            	                  ALLOCSET_DEFAULT_MINSIZE,
                	              ALLOCSET_DEFAULT_INITSIZE,
                    	          ALLOCSET_DEFAULT_MAXSIZE);

		MemoryContextSwitchTo(testMemoryContext);
	}

	assert_true(NULL != testMemoryContext &&
			CurrentMemoryContext == testMemoryContext);
}

/*
 * Cleans up memory by reseting testMemoryContext
 */
static void
TeardownDataStructures(void **state)
{
	assert_true(NULL != testMemoryContext &&
			CurrentMemoryContext == testMemoryContext);
	MemoryContextReset(testMemoryContext);
}

int
main (int argc, char* argv[]) {
	cmockery_parse_arguments(argc, argv);

	test_init_endpoints();

	const UnitTest tests[] = {
		unit_test_setup_teardown(test_etcd_write_test_function, SetupDataStructures, TeardownDataStructures),
		unit_test_setup_teardown(test_etcd_lock_test_function, SetupDataStructures, TeardownDataStructures),
		unit_test_setup_teardown(test_etcd_simple_failover_test_function, SetupDataStructures, TeardownDataStructures),
		unit_test_setup_teardown(test_etcd_failure_failover_test_function, SetupDataStructures, TeardownDataStructures)
	};
	return run_tests(tests);
}

#else

int
main(int argc, char* argv[])
{
    return 0;
}
#endif