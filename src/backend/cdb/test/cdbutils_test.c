#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"
#include "cdb/cdbutil.h"
#include "utils/etcd.h"
#include "utils/memutils.h"
#include "storage/lockdefs.h"
#include "storage/lwlock.h"
#include "common/etcdutils.h"

#ifndef USE_INTERNAL_FTS

static MemoryContext testMemoryContext = NULL;

static
void expect_heap_open_and_close(void) {
    expect_string_count(RelnameGetRelid, relname, GpSegmentConfigRelationName, -1);
	will_return_count(RelnameGetRelid, 1, -1);

    expect_value_count(table_open, relationId, 1, -1);
    expect_value_count(table_open, lockmode, AccessShareLock, -1);
    will_return_count(table_open, NULL, -1);

    expect_any_count(table_close, relation, -1);
    expect_value_count(table_close, lockmode, NoLock, -1);
    will_be_called_count(table_close, -1);
}

static
void expect_lock_acquire_and_release(void) {
    expect_value_count(LWLockAcquire, lock, CdbConfigCacheLock, -1);
	expect_value_count(LWLockAcquire, mode, LW_EXCLUSIVE, -1);
	will_return_count(LWLockAcquire, true, -1);

	expect_value_count(LWLockRelease, lock, CdbConfigCacheLock, -1);
	will_be_called_count(LWLockRelease, -1);
}

static void enable_segment_cache() {
    gp_etcd_enable_cache = true;
}

static void disable_segment_cache() {
    gp_etcd_enable_cache = false;
}

static void clean_segments_with_cache() {
    enable_segment_cache();
    cleanSegments();
    disable_segment_cache();
}

static void
SetupDataStructures(void **state)
{
    GpSegConfigEntry *config_cached = NULL;

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

        TopMemoryContext = testMemoryContext;
	}

    // Init global etcd config
    char *endpoints = GP_ETCD_TEST_SINGLE_ENDPOINTS;
    gp_etcd_endpoints = strdup(endpoints);
    gp_etcd_account_id = GP_ETCD_ACCOUNT_ID_DEFAULT;
    gp_etcd_cluster_id = GP_ETCD_CLUSTER_ID_DEFAULT;
    gp_etcd_namespace = GP_ETCD_NAMESPACE_DEFAULT;
    assert_true(NULL != testMemoryContext &&
			CurrentMemoryContext == testMemoryContext);
    
    int total_dbs = 0;
    expect_lock_acquire_and_release();
	config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    
    if (total_dbs != 0) {
        expect_heap_open_and_close();
        clean_segments_with_cache();
    }

    if (config_cached) {
        pfree(config_cached);
        config_cached = NULL;
    }
}

/*
 * cleans up memory by reseting testMemoryContext
 */
static void
TeardownDataStructures(void **state)
{
    expect_lock_acquire_and_release();
    expect_heap_open_and_close();
    clean_segments_with_cache();
    if (gp_etcd_endpoints) {
        free(gp_etcd_endpoints);
        gp_etcd_endpoints = NULL;
    }
}

void * cache_mem = NULL;

static void 
test_cdb_utils_init_cache(void **state) 
{
    /* init cache */ 
    cache_mem = malloc(ShmemSegmentConfigsCacheSize());
    memset(cache_mem, 0, ShmemSegmentConfigsCacheSize());

    expect_value(ShmemAlloc, size, ShmemSegmentConfigsCacheSize());
    will_return(ShmemAlloc, cache_mem);

    ShmemSegmentConfigsCacheAllocation();
    enable_segment_cache();
    assert_true(isSegmentConfigsCacheEnable());

    /* disable cache and check */
    disable_segment_cache();
    assert_false(isSegmentConfigsCacheEnable());
    assert_false(isSegmentConfigsCached());
}

static void 
test_cdb_utils_segment_ops(void **state) 
{
    GpSegConfigEntry *config_cached = NULL;
    int total_dbs = 0;

    disable_segment_cache();
    expect_heap_open_and_close();
    expect_lock_acquire_and_release();

    /* make sure env is clean */
	config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 0);
    assert_true(config_cached == NULL);

    /* add segment-1 */
    GpSegConfigEntry config_entry;
    config_entry.dbid = 1;
    config_entry.segindex = 0;
    config_entry.role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
    config_entry.preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
    config_entry.status = GP_SEGMENT_CONFIGURATION_STATUS_UP;
    config_entry.mode = GP_SEGMENT_CONFIGURATION_MODE_INSYNC;
    config_entry.port = 10000;
    config_entry.address = "localhost";
    config_entry.hostname = "localhost";
    config_entry.datadir = "null";    

    addSegment(&config_entry);

    /* read segment configuration cache and check */
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_int_equal(config_cached[0].dbid, config_entry.dbid);
    assert_string_equal(config_cached[0].datadir, config_entry.datadir);
    pfree(config_cached);

    /* rewrite segment-1 and segment-2 */
    rewriteSegments("1 0 p p n d 10000 localhost localhost datadir1\n"\
        "2 0 m m n u 10001 localhost localhost datadir2\n", true);

    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_int_equal(config_cached[0].dbid, 1);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_int_equal(config_cached[1].dbid, 2);
    assert_string_equal(config_cached[1].datadir, "datadir2");
    pfree(config_cached);

    /* delete segment-2 */
    delSegment(2);

    /* read segment configuration cache and check */
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_int_equal(config_cached[0].dbid, 1);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC);
    assert_true(config_cached[0].status == GP_SEGMENT_CONFIGURATION_STATUS_DOWN);
    pfree(config_cached);

    /* update segment mode and check cache have been updated */
    updateSegmentModeStatus(1, GP_SEGMENT_CONFIGURATION_MODE_INSYNC, GP_SEGMENT_CONFIGURATION_STATUS_UP);

    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_int_equal(config_cached[0].dbid, 1);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC);
    assert_true(config_cached[0].status == GP_SEGMENT_CONFIGURATION_STATUS_UP);
    pfree(config_cached);

    /* rewrite segment-51 segment-52 and make sure cache have been updated */
    rewriteSegments("51 -1 p p n u 10000 localhost localhost datadir1\n" \
        "52 -1 m m n u 10001 localhost localhost datadir2\n", true);
    activateStandby(52, 51);

    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 52);
    assert_true(config_cached[0].role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY);
    assert_true(config_cached[0].preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY);
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC);
    assert_string_equal(config_cached[0].datadir, "datadir2");
    pfree(config_cached);

    /* clean up and check */
    clean_segments_with_cache();
    assert_false(isSegmentConfigsCached());
}

static void 
test_cdb_utils_segment_cache_add_segment(void **state) 
{
    GpSegConfigEntry *config_cached = NULL;
    char * cached_buff = NULL;
    int total_dbs = 0;

    enable_segment_cache();
    expect_heap_open_and_close();
    expect_lock_acquire_and_release();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 0);
    assert_true(config_cached == NULL);

    /* add segment-1 */
    GpSegConfigEntry config_entry;
    config_entry.dbid = 1;
    config_entry.segindex = 0;
    config_entry.role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
    config_entry.preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
    config_entry.status = GP_SEGMENT_CONFIGURATION_STATUS_UP;
    config_entry.mode = GP_SEGMENT_CONFIGURATION_MODE_INSYNC;
    config_entry.port = 10000;
    config_entry.address = "localhost";
    config_entry.hostname = "localhost";
    config_entry.datadir = "null";

    addSegment(&config_entry);

    /* read segment configuration cache and check */
    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(cached_buff != NULL);
    config_cached = readGpSegConfig(cached_buff, &total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached->dbid, config_entry.dbid);
    assert_string_equal(config_cached->datadir, config_entry.datadir);
    pfree(config_cached);
    pfree(cached_buff);

    /* disable read configuration from cache and check */
    disable_segment_cache();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(config_cached->dbid, config_entry.dbid);
    assert_string_equal(config_cached->datadir, config_entry.datadir);
    pfree(config_cached);
    enable_segment_cache();


    /* add segment-100 */
    config_entry.dbid = 100;
    config_entry.datadir = "datadir-100";
    addSegment(&config_entry);

    /* read segment configuration cache and check */
    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(cached_buff != NULL);
    config_cached = readGpSegConfig(cached_buff, &total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 1);
    assert_string_equal(config_cached[0].datadir, "null");
    assert_int_equal(config_cached[1].dbid, config_entry.dbid);
    assert_string_equal(config_cached[1].datadir, config_entry.datadir);
    pfree(config_cached);
    pfree(cached_buff);

    /* disable read from cache and check */
    disable_segment_cache();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_int_equal(config_cached[0].dbid, 1);
    assert_string_equal(config_cached[0].datadir, "null");
    assert_int_equal(config_cached[1].dbid, config_entry.dbid);
    assert_string_equal(config_cached[1].datadir, config_entry.datadir);
    pfree(config_cached);
    
    /* clean up and check */
    clean_segments_with_cache();
    assert_false(isSegmentConfigsCached());
}

static void 
test_cdb_utils_segment_cache_del_segment(void **state) 
{
    GpSegConfigEntry *config_cached = NULL;
    char * cached_buff = NULL;
    int total_dbs = 0;

    disable_segment_cache();
    expect_lock_acquire_and_release();

    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 0);

    enable_segment_cache();

    expect_heap_open_and_close();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 0);
    assert_true(config_cached == NULL);

    /* rewrite segment-11 segment-21 segment-31 */
    rewriteSegments("11 0 p p n u 10000 localhost localhost datadir1\n" \
        "21 0 m m n u 10001 localhost localhost datadir2\n" \
        "31 0 m m n u 10001 localhost localhost datadir3\n", true);

    /* read from etcd cache and check */
    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 3);
    assert_true(cached_buff != NULL);

    config_cached = readGpSegConfig(cached_buff, &total_dbs);
    assert_int_equal(total_dbs, 3);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 11);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_int_equal(config_cached[1].dbid, 21);
    assert_string_equal(config_cached[1].datadir, "datadir2");
    assert_int_equal(config_cached[2].dbid, 31);
    assert_string_equal(config_cached[2].datadir, "datadir3");
    pfree(cached_buff);
    pfree(config_cached);

    /* delete segment-21 */
    delSegment(21);

    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(cached_buff != NULL);
    config_cached = readGpSegConfig(cached_buff, &total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 11);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_int_equal(config_cached[1].dbid, 31);
    assert_string_equal(config_cached[1].datadir, "datadir3");
    pfree(cached_buff);
    pfree(config_cached);

    /* disable read from cache and check */
    disable_segment_cache();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 11);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_int_equal(config_cached[1].dbid, 31);
    assert_string_equal(config_cached[1].datadir, "datadir3");
    pfree(config_cached);
    enable_segment_cache();

    /* delete segment-11 */
    delSegment(11);

    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(cached_buff != NULL);
    config_cached = readGpSegConfig(cached_buff, &total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 31);
    assert_string_equal(config_cached[0].datadir, "datadir3");
    pfree(cached_buff);
    pfree(config_cached);

    /* disable read from cache and check */
    disable_segment_cache();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 31);
    assert_string_equal(config_cached[0].datadir, "datadir3");
    pfree(config_cached);
    enable_segment_cache();

    /* delete segment-31 */
    delSegment(31);

    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 0);
    assert_true(cached_buff == NULL);

    disable_segment_cache();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 0);
    assert_true(config_cached == NULL);

    /* no need clean, cause nothing in ETCD */ 
    assert_false(isSegmentConfigsCached());
}

static void 
test_cdb_utils_segment_cache_activate_standby(void **state) 
{
    GpSegConfigEntry *config_cached = NULL;
    char * cached_buff = NULL;
    int total_dbs = 0;

    enable_segment_cache();
    expect_heap_open_and_close();
    expect_lock_acquire_and_release();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 0);
    assert_true(config_cached == NULL);

    /* rewrite master-51 and standby-52 */
    rewriteSegments("51 -1 p p n u 10000 localhost localhost datadir1\n" \
        "52 -1 m m n u 10001 localhost localhost datadir2\n", true);
    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(cached_buff != NULL);    
    pfree(cached_buff);

    /* do active standby and check */
    activateStandby(52, 51);

    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(cached_buff != NULL);
    config_cached = readGpSegConfig(cached_buff, &total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 52);
    assert_true(config_cached[0].role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY);
    assert_true(config_cached[0].preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY);
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC);
    assert_string_equal(config_cached[0].datadir, "datadir2");
    pfree(cached_buff);
    pfree(config_cached);

    /* disable read from cache and check */
    disable_segment_cache();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 1);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 52);
    assert_true(config_cached[0].role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY);
    assert_true(config_cached[0].preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY);
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC);
    assert_string_equal(config_cached[0].datadir, "datadir2");
    pfree(config_cached);
    enable_segment_cache();

    /* clean up and check */
    clean_segments_with_cache();
    assert_false(isSegmentConfigsCached());

}

static void 
test_cdb_utils_segment_cache_update_segment_mode_status(void **state) 
{
    GpSegConfigEntry *config_cached = NULL;
    char * cached_buff = NULL;
    int total_dbs = 0;

    enable_segment_cache();
    expect_heap_open_and_close();
    expect_lock_acquire_and_release();
    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 0);
    assert_true(config_cached == NULL);

    /* rewrite segment-1 and segment-2 */
    rewriteSegments("51 4 p p n u 10000 localhost localhost datadir1\n" \
        "52 4 m m s u 10001 localhost localhost datadir2\n", true);

    /* update segment mode and check cache have been updated */
    updateSegmentModeStatus(51, GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC, GP_SEGMENT_CONFIGURATION_STATUS_DOWN);

    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(cached_buff != NULL);
    config_cached = readGpSegConfig(cached_buff, &total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 51);
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC);
    assert_true(config_cached[0].status == GP_SEGMENT_CONFIGURATION_STATUS_DOWN);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_int_equal(config_cached[1].dbid, 52);
    assert_true(config_cached[1].mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC);
    assert_true(config_cached[1].status == GP_SEGMENT_CONFIGURATION_STATUS_UP);
    assert_string_equal(config_cached[1].datadir, "datadir2");
    pfree(cached_buff);
    pfree(config_cached);

    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 51);
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC);
    assert_true(config_cached[0].status == GP_SEGMENT_CONFIGURATION_STATUS_DOWN);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_int_equal(config_cached[1].dbid, 52);
    assert_true(config_cached[1].mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC);
    assert_true(config_cached[1].status == GP_SEGMENT_CONFIGURATION_STATUS_UP);
    assert_string_equal(config_cached[1].datadir, "datadir2");
    pfree(config_cached);

    updateSegmentModeStatus(51, GP_SEGMENT_CONFIGURATION_MODE_INSYNC, GP_SEGMENT_CONFIGURATION_STATUS_UP);
    cached_buff = readSegmentConfigsCache(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(cached_buff != NULL);
    config_cached = readGpSegConfig(cached_buff, &total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 51);
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC);
    assert_true(config_cached[0].status == GP_SEGMENT_CONFIGURATION_STATUS_UP);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_int_equal(config_cached[1].dbid, 52);
    assert_true(config_cached[1].mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC);
    assert_true(config_cached[1].status == GP_SEGMENT_CONFIGURATION_STATUS_UP);
    assert_string_equal(config_cached[1].datadir, "datadir2");
    pfree(cached_buff);
    pfree(config_cached);

    config_cached = readGpSegConfigFromETCDAllowNull(&total_dbs);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(total_dbs, 2);
    assert_true(config_cached != NULL);
    assert_int_equal(config_cached[0].dbid, 51);
    assert_true(config_cached[0].mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC);
    assert_true(config_cached[0].status == GP_SEGMENT_CONFIGURATION_STATUS_UP);
    assert_string_equal(config_cached[0].datadir, "datadir1");
    assert_int_equal(config_cached[1].dbid, 52);
    assert_true(config_cached[1].mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC);
    assert_true(config_cached[1].status == GP_SEGMENT_CONFIGURATION_STATUS_UP);
    assert_string_equal(config_cached[1].datadir, "datadir2");
    pfree(config_cached);

    clean_segments_with_cache();
    assert_false(isSegmentConfigsCached());
}


int
main(int argc, char* argv[])
{
    int rc;
	cmockery_parse_arguments(argc, argv);

    disable_segment_cache();
	const UnitTest tests[] = {
        /* test_cdb_utils_init_cache must be first test case
         * other tests will used segment configuration cache which need init in test_cdb_utils_init_cache
         */ 
        unit_test_setup_teardown(test_cdb_utils_init_cache, SetupDataStructures, TeardownDataStructures),
		unit_test_setup_teardown(test_cdb_utils_segment_ops, SetupDataStructures, TeardownDataStructures),
        unit_test_setup_teardown(test_cdb_utils_segment_cache_add_segment, SetupDataStructures, TeardownDataStructures),
        unit_test_setup_teardown(test_cdb_utils_segment_cache_del_segment, SetupDataStructures, TeardownDataStructures),
        unit_test_setup_teardown(test_cdb_utils_segment_cache_activate_standby, SetupDataStructures, TeardownDataStructures),
        unit_test_setup_teardown(test_cdb_utils_segment_cache_update_segment_mode_status, SetupDataStructures, TeardownDataStructures)
	};
	rc = run_tests(tests);

    /* clean memory at last */ 
	MemoryContextReset(testMemoryContext);
    testMemoryContext = NULL;
    TopMemoryContext = NULL;

    /* release cache buffer */ 
    if (cache_mem)
        free(cache_mem);

    return rc;
}
#else

int
main(int argc, char* argv[])
{
    return 0;
}
#endif