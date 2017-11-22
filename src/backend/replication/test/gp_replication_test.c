#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"

#define Assert(condition) if (!condition) AssertFailed()

bool is_assert_failed = false;

void AssertFailed()
{
	is_assert_failed = true;
}

/* Actual function body */
#include "../gp_replication.c"

static void
expect_lwlock(LWLockMode lockmode)
{
	expect_value(LWLockAcquire, lockid, SyncRepLock);
	expect_value(LWLockAcquire, mode, lockmode);
	will_be_called(LWLockAcquire);

	expect_value(LWLockRelease, lockid, SyncRepLock);
	will_be_called(LWLockRelease);
}

static void
test_setup(WalSndCtlData *data, WalSndState state)
{
	max_wal_senders = 1;
	WalSndCtl = data;
	data->walsnds[0].pid = 1;
	data->walsnds[0].state = state;

	expect_lwlock(LW_SHARED);
}

void
test_GetMirrorStatus_Pid_Zero(void **state)
{
	bool isMirrorUp;
	bool isInSync;
	WalSndCtlData data;

	max_wal_senders = 1;
	WalSndCtl = &data;
	data.walsnds[0].pid = 0;

	expect_lwlock(LW_SHARED);
	GetMirrorStatus(&isMirrorUp, &isInSync);

	assert_false(isMirrorUp);
	assert_false(isInSync);
}

void
test_GetMirrorStatus_WALSNDSTATE_STARTUP(void **state)
{
	bool isMirrorUp;
	bool isInSync;
	WalSndCtlData data;

	test_setup(&data, WALSNDSTATE_STARTUP);
	GetMirrorStatus(&isMirrorUp, &isInSync);

	assert_false(isMirrorUp);
	assert_false(isInSync);
}

void
test_GetMirrorStatus_WALSNDSTATE_BACKUP(void **state)
{
	bool isMirrorUp;
	bool isInSync;
	WalSndCtlData data;

	test_setup(&data, WALSNDSTATE_BACKUP);
	GetMirrorStatus(&isMirrorUp, &isInSync);

	assert_false(isMirrorUp);
	assert_false(isInSync);
}

void
test_GetMirrorStatus_WALSNDSTATE_CATCHUP(void **state)
{
	bool isMirrorUp;
	bool isInSync;
	WalSndCtlData data;

	test_setup(&data, WALSNDSTATE_CATCHUP);
	GetMirrorStatus(&isMirrorUp, &isInSync);

	assert_true(isMirrorUp);
	assert_false(isInSync);
}

void
test_GetMirrorStatus_WALSNDSTATE_STREAMING(void **state)
{
	bool isMirrorUp;
	bool isInSync;
	WalSndCtlData data;

	test_setup(&data, WALSNDSTATE_STREAMING);
	GetMirrorStatus(&isMirrorUp, &isInSync);

	assert_true(isMirrorUp);
	assert_true(isInSync);
}

void
test_SetSyncStandbysDefined(void **state)
{
	WalSndCtlData data;
	WalSndCtl = &data;
	data.sync_standbys_defined = false;

	expect_lwlock(LW_EXCLUSIVE);
#ifdef USE_ASSERT_CHECKING
	expect_value(LWLockHeldByMe, lockid, SyncRepLock);
	will_return(LWLockHeldByMe, true);
#endif

	/*
	 * set_gp_replication_config() should only be called once when mirror first
	 * comes up to set synchronous wal rep state
	 */
	expect_string_count(set_gp_replication_config, name, "synchronous_standby_names", 1);
	expect_string_count(set_gp_replication_config, value, "*", 1);
	will_be_called(set_gp_replication_config);

	/* simulate first call when mirror first comes up */
	assert_false(WalSndCtl->sync_standbys_defined);
	assert_true(SyncRepStandbyNames == NULL);
	SetSyncStandbysDefined();

	/* relative variables should have updated */
	assert_true(WalSndCtl->sync_standbys_defined);
	assert_true(strcmp(SyncRepStandbyNames, "*") == 0);

	expect_lwlock(LW_EXCLUSIVE);

	/* simulate second call after sync state is set which should do nothing */
	SetSyncStandbysDefined();
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_GetMirrorStatus_Pid_Zero),
		unit_test(test_GetMirrorStatus_WALSNDSTATE_STARTUP),
		unit_test(test_GetMirrorStatus_WALSNDSTATE_BACKUP),
		unit_test(test_GetMirrorStatus_WALSNDSTATE_CATCHUP),
		unit_test(test_GetMirrorStatus_WALSNDSTATE_STREAMING),
		unit_test(test_SetSyncStandbysDefined)
	};
	return run_tests(tests);
}
