#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

/* Actual function body */
#include "../fts.c"

/*
 * Function to help create representation of gp_segment_configuration, for
 * ease of testing different scenarios. Using the same can mock out different
 * configuration layouts.
 *
 * --------------------------------
 * Inputs:
 *    segCnt - number of primary segments the configuration should mock
 *    has_mirrors - controls if mirrors corresponding to primary are created
 * --------------------------------
 *
 * Function always adds master to the configuration. Also, all the segments
 * are by default marked up. Tests leverage to create initial configuration
 * using this and then modify the same as per needs to mock different
 * scenarios like mirror down, primary down, etc...
 */
static CdbComponentDatabases *
InitTestCdb(int segCnt, bool has_mirrors)
{
	int			i = 0;
	int 		mirror_multiplier = 1;

	if (has_mirrors)
		mirror_multiplier = 2;

	CdbComponentDatabases *cdb =
			(CdbComponentDatabases *) palloc(sizeof(CdbComponentDatabases));

	cdb->total_entry_dbs = 1;
	cdb->total_segment_dbs = segCnt * mirror_multiplier;	/* with mirror? */
	cdb->total_segments = segCnt;
	cdb->my_dbid = 1;
	cdb->my_segindex = -1;
	cdb->my_isprimary = true;
	cdb->entry_db_info = palloc(
			sizeof(CdbComponentDatabaseInfo) * cdb->total_entry_dbs);
	cdb->segment_db_info = palloc(
			sizeof(CdbComponentDatabaseInfo) * cdb->total_segment_dbs);


	/* create the master entry_db_info */
	CdbComponentDatabaseInfo *cdbinfo = &cdb->entry_db_info[0];

	cdbinfo->dbid = 1;
	cdbinfo->segindex = '-1';

	cdbinfo->role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
	cdbinfo->preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
	cdbinfo->status = GP_SEGMENT_CONFIGURATION_STATUS_UP;

	/* create the segment_db_info entries */
	for (i = 0; i < cdb->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *cdbinfo = &cdb->segment_db_info[i];


		cdbinfo->dbid = i + 2;
		cdbinfo->segindex = i / 2;

		if (has_mirrors)
		{
			cdbinfo->role = i % 2 ?
							GP_SEGMENT_CONFIGURATION_ROLE_MIRROR :
							GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;

			cdbinfo->preferred_role = i % 2 ?
									  GP_SEGMENT_CONFIGURATION_ROLE_MIRROR :
									  GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
		}
		else
		{
			cdbinfo->role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
			cdbinfo->preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
		}
		cdbinfo->status = GP_SEGMENT_CONFIGURATION_STATUS_UP;

	}

	return cdb;
}

static CdbComponentDatabaseInfo*
GetSegmentFromCdbComponentDatabases(CdbComponentDatabases *dbs,
									int16 segindex, char role)
{
	int i;

	for(i = 0; i < dbs->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *cdb = &dbs->segment_db_info[i];
		if(cdb->segindex == segindex && cdb->role == role)
			return cdb;
	}
	return NULL;
}

/* mock FtsIsActive */
void
MockFtsIsActive(bool expected_return_value)
{
	static FtsProbeInfo fts_info;

	ftsProbeInfo = &fts_info;

	if (shutdown_requested)
	{
		ftsProbeInfo->fts_discardResults = false;
	}
	else
	{
		ftsProbeInfo->fts_discardResults = !expected_return_value;
	}
}


static int
CheckConfigVals(const Datum *value, /* from the function under test */
				const Datum *check_value) /* from the test case */
{
	return datumIsEqual(value[Anum_gp_segment_configuration_status-1],
						check_value[Anum_gp_segment_configuration_status-1],
						true, false);
}

static int
CheckHistrelVals(const Datum *value, /* from the function under test */
				 const Datum *check_value) /* from the test case */
{
	return datumIsEqual(value[Anum_gp_configuration_history_dbid-1],
						check_value[Anum_gp_configuration_history_dbid-1],
						true, false) &&
		   datumIsEqual(value[Anum_gp_configuration_history_desc-1],
						check_value[Anum_gp_configuration_history_desc-1],
						false, -1);
}

static void
probeWalRepUpdateConfig_will_be_called_with(
		int16 dbid,
		int16 segindex,
		bool IsSegmentAlive)
{
	/* Mock heap_open gp_configuration_history_relation */
	static RelationData gp_configuration_history_relation;
	expect_value(heap_open, relationId, GpConfigHistoryRelationId);
	expect_any(heap_open, lockmode);
	will_return(heap_open, &gp_configuration_history_relation);

	/* Mock heap_open gp_segment_configuration_relation */
	static RelationData gp_segment_configuration_relation;
	expect_value(heap_open, relationId, GpSegmentConfigRelationId);
	expect_any(heap_open, lockmode);
	will_return(heap_open, &gp_segment_configuration_relation);


	char desc[SQL_CMD_BUF_SIZE];
	Datum *histvals = palloc(sizeof(Datum) * Natts_gp_configuration_history);

	histvals[Anum_gp_configuration_history_dbid-1] =
			Int16GetDatum(dbid);

	snprintf(desc, sizeof(desc),
			 "FTS: update status for dbid %d with contentid %d to %c",
			 dbid, segindex,
			 IsSegmentAlive ? GP_SEGMENT_CONFIGURATION_STATUS_UP :
			 GP_SEGMENT_CONFIGURATION_STATUS_DOWN);

	histvals[Anum_gp_configuration_history_desc-1] =
			CStringGetTextDatum(desc);


	/* Mock heap_form_tuple inline function */
	expect_any_count(heaptuple_form_to, tupleDescriptor, 1);
	expect_check(heaptuple_form_to, values, CheckHistrelVals, histvals);
	expect_any_count(heaptuple_form_to, isnull, 1);
	expect_any_count(heaptuple_form_to, dst, 1);
	expect_any_count(heaptuple_form_to, dstlen, 1);
	will_be_called_count(heaptuple_form_to, 1);

	/* Mock simple_heap_insert */
	expect_any_count(simple_heap_insert, relation, 1);
	expect_any_count(simple_heap_insert, tup, 1);
	will_be_called_count(simple_heap_insert, 1);

	/* Mock CatalogUpdateIndexes */
	expect_any_count(CatalogUpdateIndexes, heapRel, 2);
	expect_any_count(CatalogUpdateIndexes, heapTuple, 2);
	will_be_called_count(CatalogUpdateIndexes, 2);

	/* Mock heap_close */
	expect_any_count(relation_close, relation, 2);
	expect_any_count(relation_close, lockmode, 2);
	will_be_called_count(relation_close, 2);

	/* Mock ScanKeyInit */
	expect_any_count(ScanKeyInit, entry, 1);
	expect_any_count(ScanKeyInit, attributeNumber, 1);
	expect_any_count(ScanKeyInit, strategy, 1);
	expect_any_count(ScanKeyInit, procedure, 1);
	expect_value_count(ScanKeyInit, argument, dbid, 1);
	will_be_called_count(ScanKeyInit, 1);

	/* Mock systable_beginscan */
	expect_any_count(systable_beginscan, heapRelation, 1);
	expect_any_count(systable_beginscan, indexId, 1);
	expect_any_count(systable_beginscan, indexOK, 1);
	expect_any_count(systable_beginscan, snapshot, 1);
	expect_any_count(systable_beginscan, nkeys, 1);
	expect_any_count(systable_beginscan, key, 1);
	will_be_called_count(systable_beginscan, 1);

	static HeapTupleData config_tuple;
	expect_any(systable_getnext, sysscan);
	will_return(systable_getnext, &config_tuple);

	Datum *configvals = palloc(sizeof(Datum) * Natts_gp_segment_configuration);
	configvals[Anum_gp_segment_configuration_status-1] =
			CharGetDatum(IsSegmentAlive ? GP_SEGMENT_CONFIGURATION_STATUS_UP :
						 GP_SEGMENT_CONFIGURATION_STATUS_DOWN);
	HeapTuple new_tuple = palloc(sizeof(HeapTupleData));
	expect_any(heap_modify_tuple, tuple);
	expect_any(heap_modify_tuple, tupleDesc);
	expect_check(heap_modify_tuple, replValues, CheckConfigVals, configvals);
	expect_any(heap_modify_tuple, replIsnull);
	expect_any(heap_modify_tuple, doReplace);
	will_return(heap_modify_tuple, new_tuple);

	expect_any_count(simple_heap_update, relation, 1);
	expect_any_count(simple_heap_update, otid, 1);
	expect_any_count(simple_heap_update, tup, 1);
	will_be_called_count(simple_heap_update, 1);

	expect_any_count(systable_endscan, sysscan, 1);
	will_be_called_count(systable_endscan, 1);
}

static void
MockPrimaryAndMirrorProbeResponse(CdbComponentDatabases *cdbs,
								  probe_response_per_segment *response,
								  bool UpdatePrimary, bool UpdateMirror)
{
	CdbComponentDatabaseInfo *primary = response->segment_db_info;

	CdbComponentDatabaseInfo *mirror =
		FtsGetPeerSegment(cdbs, primary->segindex, primary->dbid);

	/* mock FtsIsActive true */
	MockFtsIsActive(true);

	/* mock probeWalRepUpdateConfig */
	if (UpdatePrimary) {
		probeWalRepUpdateConfig_will_be_called_with(
				primary->dbid,
				primary->segindex,
				response->result.isPrimaryAlive);
	}
	else if (UpdateMirror)
	{
		probeWalRepUpdateConfig_will_be_called_with(
				mirror->dbid,
				mirror->segindex,
				response->result.isMirrorAlive);
	}
}

static void
PrimaryOrMirrorWillBeUpdated(int count)
{

	will_be_called_count(StartTransactionCommand, count);
	will_be_called_count(GetTransactionSnapshot, count);
	will_be_called_count(CommitTransactionCommand, count);
}

/* Case: 0 segments, is_updated is always false */
void
test_probeWalRepPublishUpdate_for_zero_segment(void **state)
{
	probe_context context;

	context.count = 0;

	bool is_updated = probeWalRepPublishUpdate(NULL, &context);

	assert_false(is_updated);
}

/*
 * Case: 1 segment, is_updated is false, because FtsIsActive failed
 */
void
test_probeWalRepPublishUpdate_for_FtsIsActive_false(void **state)
{
	probe_context context;
	context.count = 1;
	probe_response_per_segment response;
	context.responses = &response;
	CdbComponentDatabaseInfo info;
	response.segment_db_info = &info;
	info.role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;

	/* mock FtsIsActive false */
	MockFtsIsActive(false);

	bool is_updated = probeWalRepPublishUpdate(NULL, &context);

	assert_false(is_updated);
}

/*
 * Case: 1 segment, is_updated is false, because shutdown_requested is true.
 */
void
test_probeWalRepPublishUpdate_for_shutdown_requested(void **state)
{
	probe_context context;
	context.count = 1;
	probe_response_per_segment response;
	context.responses = &response;
	CdbComponentDatabaseInfo info;
	response.segment_db_info = &info;
	info.role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;

	/* mock FtsIsActive true */
	shutdown_requested = true;
	MockFtsIsActive(true);

	bool is_updated = probeWalRepPublishUpdate(NULL, &context);

	/* restore the original value to let the rest of the test pass */
	shutdown_requested = false;

	assert_false(is_updated);
}

/*
 * Case: 2 segments, is_updated is false, because neither primary nor mirror
 * updated
 */
void
test_probeWalRepPublishUpdate_no_update(void **state)
{
	probe_context context;
	CdbComponentDatabases *cdb_component_dbs = InitTestCdb(2, true);
	FtsWalRepInitProbeContext(cdb_component_dbs, &context);

	/* probeWalRepPublishUpdate should not update a probe state */
	bool is_updated = probeWalRepPublishUpdate(cdb_component_dbs, &context);

	assert_false(is_updated);
	pfree(cdb_component_dbs);
}

/*
 * Case: 2 segments, is_updated is true, because content 0 primary is updated
 */
void
test_probeWalRepPublishUpdate_primary_up_to_down(void **state)
{
	probe_context context;
	CdbComponentDatabases *cdb_component_dbs = InitTestCdb(2, true);
	FtsWalRepInitProbeContext(cdb_component_dbs, &context);

	/* No response received from first segment */

	/* Response received from second segment */
	context.responses[1].result.isPrimaryAlive = true;

	/* Only one segment will be updated */
	PrimaryOrMirrorWillBeUpdated(1);

	/* First segment will be updated and marked as down */
	MockPrimaryAndMirrorProbeResponse(cdb_component_dbs, &context.responses[0], true, false);

	bool is_updated = probeWalRepPublishUpdate(cdb_component_dbs, &context);

	assert_true(is_updated);
	pfree(cdb_component_dbs);
}

/*
 * Case: 2 segments, is_updated is true, because content 0 mirror is updated
 */
void
test_probeWalRepPublishUpdate_mirror_up_to_down(void **state)
{
	probe_context context;
	CdbComponentDatabases *cdb_component_dbs = InitTestCdb(2, true);
	FtsWalRepInitProbeContext(cdb_component_dbs, &context);

	/*
	 * Test response received from both segments. First primary's mirror is
	 * reported as DOWN.
	 */
	context.responses[0].result.isPrimaryAlive = true;
	context.responses[0].result.isMirrorAlive = false;

	context.responses[1].result.isPrimaryAlive = true;


	/* the mirror will be updated */
	PrimaryOrMirrorWillBeUpdated(1);
	MockPrimaryAndMirrorProbeResponse(cdb_component_dbs, &context.responses[0], false, true);

	bool is_updated = probeWalRepPublishUpdate(cdb_component_dbs, &context);

	assert_true(is_updated);
	pfree(cdb_component_dbs);
}

/*
 * Case: 3 segments, is_updated is true, because content 0 mirror is down and
 * probe response is up
 */
void
test_probeWalRepPublishUpdate_mirror_down_to_up(void **state)
{
	probe_context context;
	CdbComponentDatabases *cdb_component_dbs = InitTestCdb(3, true);

	/* set the mirror down in config */
	CdbComponentDatabaseInfo *cdbinfo =
			GetSegmentFromCdbComponentDatabases(
				cdb_component_dbs, 0, GP_SEGMENT_CONFIGURATION_ROLE_MIRROR);
	cdbinfo->status = GP_SEGMENT_CONFIGURATION_STATUS_DOWN;

	FtsWalRepInitProbeContext(cdb_component_dbs, &context);

	/*
	 * Response received from all three segments, DOWN mirror is reported UP
	 * for first primary.
	 */
	context.responses[0].result.isPrimaryAlive = true;
	context.responses[0].result.isMirrorAlive = true;

	/* no change */
	context.responses[1].result.isPrimaryAlive = true;
	context.responses[2].result.isPrimaryAlive = true;

	/* the mirror will be updated */
	PrimaryOrMirrorWillBeUpdated(1);
	MockPrimaryAndMirrorProbeResponse(cdb_component_dbs, &context.responses[0], false, true);

	bool is_updated = probeWalRepPublishUpdate(cdb_component_dbs, &context);

	assert_true(is_updated);
	pfree(cdb_component_dbs);
}

/*
 * Case: 4 segments, is_updated is true, as we are changing the state of
 * several segment pairs.
 */
void
test_probeWalRepPublishUpdate_multiple_segments(void **state)
{
	probe_context context;
	CdbComponentDatabases *cdb_component_dbs = InitTestCdb(4, true);

	/*
	 * Mark the mirror for content 0 down in configuration, probe response
	 * indicates it's up.
	 */
	CdbComponentDatabaseInfo *cdbinfo =
			GetSegmentFromCdbComponentDatabases(
				cdb_component_dbs, 0, GP_SEGMENT_CONFIGURATION_ROLE_MIRROR);
	cdbinfo->status = GP_SEGMENT_CONFIGURATION_STATUS_DOWN;

	FtsWalRepInitProbeContext(cdb_component_dbs, &context);

	/* First segment DOWN mirror, now reported UP */
	context.responses[0].result.isPrimaryAlive = true;
	context.responses[0].result.isMirrorAlive = true;

	/* Second segment UP mirror, now reported DOWN */
	context.responses[1].result.isPrimaryAlive = true;
	context.responses[1].result.isMirrorAlive = false;

	/* Third segment no response received */

	/* Fourth segment, response received no change */
	context.responses[3].result.isPrimaryAlive = true;

	/* we are updating three of the four segments */
	PrimaryOrMirrorWillBeUpdated(3);

	/* the mirror will be updated */
	MockPrimaryAndMirrorProbeResponse(cdb_component_dbs, &context.responses[0], false, true);
	/* the mirror will be updated */
	MockPrimaryAndMirrorProbeResponse(cdb_component_dbs, &context.responses[1], false, true);
	/* the primary will be updated */
	MockPrimaryAndMirrorProbeResponse(cdb_component_dbs, &context.responses[2], true, false);
	/* the fourth segment will not change status */

	bool is_updated = probeWalRepPublishUpdate(cdb_component_dbs, &context);

	assert_true(is_updated);
	pfree(cdb_component_dbs);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_probeWalRepPublishUpdate_for_zero_segment),
		unit_test(test_probeWalRepPublishUpdate_for_shutdown_requested),
		unit_test(test_probeWalRepPublishUpdate_for_FtsIsActive_false),
		unit_test(test_probeWalRepPublishUpdate_no_update),
		unit_test(test_probeWalRepPublishUpdate_primary_up_to_down),
		unit_test(test_probeWalRepPublishUpdate_mirror_up_to_down),
		unit_test(test_probeWalRepPublishUpdate_mirror_down_to_up),
		unit_test(test_probeWalRepPublishUpdate_multiple_segments)
	};

	MemoryContextInit();

	return run_tests(tests);
}
