/*
 * datalake_fdw.c
 *		  Foreign-data wrapper for dataLake (Platform Extension Framework)
 *
 */


#include "datalake_def.h"
#include "datalake_option.h"
#include "datalake_fragment.h"
#include "common/fileSystemWrapper.h"
#include "common/partition_selector.h"
#include "src/dlproxy/datalake.h"

#include "postgres.h"

#include "access/formatter.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/detoast.h"
#include "tcop/tcopprot.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbdisp.h"
#include "commands/copy.h"
#if (PG_VERSION_NUM >= 140000)
#include "commands/copyfrom_internal.h"
#include "commands/copyto_internal.h"
#endif
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "parser/parsetree.h"
#if (PG_VERSION_NUM >= 140000)
#include "utils/backend_progress.h"
#endif
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/sampling.h"
#include "utils/typcache.h"
#include "utils/acl.h"


PG_MODULE_MAGIC;

#define DATALAKE_SEGMENT_ID                 GpIdentity.segindex
#define DATALAKE_SEGMENT_COUNT              getgpsegmentCount()
#define EXEC_FLAG_VECTOR 0x8000
#define FIX_ATTR_NUM  3

extern Datum datalake_fdw_handler(PG_FUNCTION_ARGS);

extern Bitmapset **acquire_func_colLargeRowIndexes;


/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(datalake_fdw_handler);
PG_FUNCTION_INFO_V1(datalake_acquire_sample_rows);

/*
 * FDW functions declarations
 */
static void dataLakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

#if PG_VERSION_NUM >= 90500
static ForeignScan *
dataLakeGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
				  List *scan_clauses,
				  Plan *outer_plan);
#else
static ForeignScan *
dataLakeGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,	/* target list */
				  List *scan_clauses);
#endif

static void dataLakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

static void dataLakeExplainForeignScan(ForeignScanState *node, ExplainState *es);

static void dataLakeBeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *dataLakeIterateForeignScan(ForeignScanState *node);

static void dataLakeReScanForeignScan(ForeignScanState *node);

static void dataLakeEndForeignScan(ForeignScanState *node);

/* Foreign updates */
static void dataLakeBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdw_private, int subplan_index, int eflags);

static TupleTableSlot *dataLakeExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot);

static void dataLakeEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo);

static int dataLakeIsForeignRelUpdatable(Relation rel);

static bool dataLakeIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
							  RangeTblEntry *rte);

static int	dataLakeAcquireSampleRowsFunc(Relation relation, int elevel,
										  HeapTuple *rows, int targRows,
										  double *totalRows,
										  double *totalDeadRows);
static bool dataLakeAnalyzeForeignTable(Relation relation,
										AcquireSampleRowsFunc *func,
										BlockNumber *totalPages);
static void costDataLakeScan(ForeignPath *path, PlannerInfo *root,
				 RelOptInfo *baserel, ParamPathInfo *param_info);

/*
 * Helper functions
 */
static void InitCopyState(dataLakeFdwScanState *sstate);

int CopyRead(void *outbuf, int minlen, int maxlen, void *extra);

void initScanStatue(dataLakeFdwScanState *dataLakesstate);

void iterateScanStatus(dataLakeFdwScanState *dataLakesstate, TupleTableSlot *slot);

void iterateRecordBatch(dataLakeFdwScanState *dataLakesstate, VirtualTupleTableSlot *vslot);

void endScanStatus(dataLakeFdwScanState *dataLakesstate);

void freeFdwPrivateList(List *fdw_private);

void freeFdwPrivatePartitionList(List *fdw_private);

void freeFdwPrivate(dataLakeFdwScanState *sstate, ForeignScan *foreignScan);

static void initModify(dataLakeFdwScanState *dataLakesstate);

static void initCopyStateForModify(dataLakeFdwScanState *dataLakesstate);

static void insertModify(dataLakeFdwScanState *dataLakesstate, TupleTableSlot *slot);

static void endModify(dataLakeFdwScanState *dataLakesstate);

bool hasZeorSelectedPartition(dataLakeFdwScanState *dataLakesstate);

#if (PG_VERSION_NUM < 140000)
static CopyState
BeginCopyTo(Relation forrel, List *options);
#else
static CopyToState
BeginCopyToModify(Relation forrel, List *options);
#endif

#if (PG_VERSION_NUM >= 140000)
static void EndCopyScan(CopyFromState cstate);
static void EndCopyModify(CopyToState cstate);
#endif

/*
 * Workspace for analyzing a foreign table.
 */
typedef struct DataLakeAnalyzeState
{
	/* collected sample rows */
	HeapTuple  *rows;			/* array of size targrows */
	int			targrows;		/* target # of sample rows */
	int			numrows;		/* # of sample rows collected */

	/* for random sampling */
	double		samplerows;		/* # of rows fetched */
	double		rowstoskip;		/* # of rows to skip before next sample */
	ReservoirStateData rstate;	/* state for reservoir sampling */

	/* working memory contexts */
	MemoryContext anl_cxt;		/* context for per-analyze lifespan data */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} DataLakeAnalyzeState;

/*
 * Foreign-data wrapper handler functions:
 * returns a struct with pointers to the
 * datalake_fdw callback routines.
 */
Datum
datalake_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	/*
	 * foreign table scan support
	 */

	/* master - only */
	fdw_routine->GetForeignRelSize = dataLakeGetForeignRelSize;
	fdw_routine->GetForeignPaths = dataLakeGetForeignPaths;
	fdw_routine->GetForeignPlan = dataLakeGetForeignPlan;

	fdw_routine->ExplainForeignScan = dataLakeExplainForeignScan;

	/* segment - only when mpp_execute = segments */
	fdw_routine->BeginForeignScan = dataLakeBeginForeignScan;
	fdw_routine->IterateForeignScan = dataLakeIterateForeignScan;
	fdw_routine->ReScanForeignScan = dataLakeReScanForeignScan;
	fdw_routine->EndForeignScan = dataLakeEndForeignScan;

	/*
	 * foreign table insert support
	 */

	/*
	 * AddForeignUpdateTargets set to NULL, no extra target expressions are
	 * added
	 */
	fdw_routine->AddForeignUpdateTargets = NULL;

	/*
	 * PlanForeignModify set to NULL, no additional plan-time actions are
	 * taken
	 */
	fdw_routine->PlanForeignModify = NULL;
	fdw_routine->BeginForeignModify = dataLakeBeginForeignModify;
	fdw_routine->ExecForeignInsert = dataLakeExecForeignInsert;

	/*
	 * ExecForeignUpdate and ExecForeignDelete set to NULL since updates and
	 * deletes are not supported
	 */
	fdw_routine->ExecForeignUpdate = NULL;
	fdw_routine->ExecForeignDelete = NULL;
	fdw_routine->EndForeignModify = dataLakeEndForeignModify;
	fdw_routine->IsForeignRelUpdatable = dataLakeIsForeignRelUpdatable;
	fdw_routine->IsForeignScanParallelSafe = dataLakeIsForeignScanParallelSafe;


	/* Support functions for ANALYZE */
	fdw_routine->AnalyzeForeignTable = dataLakeAnalyzeForeignTable;

	PG_RETURN_POINTER(fdw_routine);
}

static void
extract_used_attributes(RelOptInfo *baserel)
{
    dataLakeFdwPlanState *fdw_private = (dataLakeFdwPlanState *) baserel->fdw_private;
    ListCell *lc;

    pull_varattnos((Node *) baserel->reltarget->exprs,
                   baserel->relid,
                   &fdw_private->attrs_used);

    foreach(lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        pull_varattnos((Node *) rinfo->clause,
                       baserel->relid,
                       &fdw_private->attrs_used);
    }

    if (bms_is_empty(fdw_private->attrs_used))
    {
        bms_free(fdw_private->attrs_used);
        fdw_private->attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
    }
}

static void
deparseTargetList(Relation rel, Bitmapset *attrs_used, List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	int			i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

	if (have_wholerow)
		return;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}
}

/*
 * GetForeignRelSize
 *		set relation size estimates for a foreign table
 */
static void
dataLakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	dataLakeFdwPlanState* fdw_private = (dataLakeFdwPlanState *) palloc0(sizeof(dataLakeFdwPlanState));
	baserel->fdw_private = fdw_private;

	set_baserel_size_estimates(root, baserel);
}

/*
 * dataLakeGetForeignPaths
 */
static void
dataLakeGetForeignPaths(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid)
{
	ForeignPath *path = NULL;
	int			total_cost = 50000;
	Relation	rel;
	dataLakeFdwPlanState *fdw_private = (dataLakeFdwPlanState*)baserel->fdw_private;

	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

	rel = table_open(rte->relid, NoLock);


	/* Collect used attributes to reduce number of read columns during scan */
    extract_used_attributes(baserel);

	deparseTargetList(rel, fdw_private->attrs_used, &fdw_private->retrieved_attrs);

	path = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
								   NULL,	/* default pathtarget */
#endif
								   baserel->rows,
								   50000,
								   total_cost,
								   NIL, /* no pathkeys */
								   NULL,	/* no outer rel either */
#if PG_VERSION_NUM >= 90500
								   NULL,	/* no extra plan */
#endif
								   NULL);
	heap_close(rel, NoLock);
	/*
	 * Create a ForeignPath node and add it as only possible path.
	 */

	costDataLakeScan(path, root, baserel, path->path.param_info);

	add_path(baserel, (Path *) path, root);
	set_cheapest(baserel);
}

/*
 * Build a target list (ie, a list of TargetEntry) for the Path's output.
 *
 * This is almost just make_tlist_from_pathtarget(), but we also have to
 * deal with replacing nestloop params.
 */
static List *
build_path_tlist(PlannerInfo *root, Path *path)
{
	List	   *tlist = NIL;
	Index	   *sortgrouprefs = path->pathtarget->sortgrouprefs;
	int			resno = 1;
	ListCell   *v;

	foreach(v, path->pathtarget->exprs)
	{
		Node	   *node = (Node *) lfirst(v);
		TargetEntry *tle;

		/*
		 * If it's a parameterized path, there might be lateral references in
		 * the tlist, which need to be replaced with Params.  There's no need
		 * to remake the TargetEntry nodes, so apply this to each list item
		 * separately.
		 */
		/* Not implemented yet! */
		if (path->param_info)
			Assert(false);

		tle = makeTargetEntry((Expr *) node,
							  resno,
							  NULL,
							  false);
		if (sortgrouprefs)
			tle->ressortgroupref = sortgrouprefs[resno - 1];

		tlist = lappend(tlist, tle);
		resno++;
	}
	return tlist;
}

/*
 * GetForeignPlan
 *		create a ForeignScan plan node
 */
#if PG_VERSION_NUM >= 90500
static ForeignScan *
dataLakeGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
				  List *scan_clauses,
				  Plan *outer_plan)
#else
static ForeignScan *
dataLakeGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,	/* target list */
				  List *scan_clauses)
#endif
{
	dataLakeFdwPlanState *fdw_private = (dataLakeFdwPlanState*)baserel->fdw_private;

	Index		scan_relid = baserel->relid;


	scan_clauses = extract_actual_clauses(scan_clauses, false);


	List* private_lists = list_make2(makeString("scan"), fdw_private->retrieved_attrs);
	/* Planner prefers physical tlist (the targetlist containing all Vars in order) for ForiegnScan,
	 * so as to allow self-defined optimization. So we need build tlist by path to extract the Vars we
	 * actually need.
	 */
	List *fdwtlist = build_path_tlist(root, &best_path->path);

	return make_foreignscan(
							fdwtlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							private_lists
	#if PG_VERSION_NUM >= 90500
								,NIL
								,NIL
								,outer_plan
	#endif
	);
}


/*
 * dataLakeExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
dataLakeExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	elog(DEBUG5, "datalake_fdw: dataLakeExplainForeignScan starts on segment: %d", DATALAKE_SEGMENT_ID);

	/* TODO: make this a meaningful callback */

	elog(DEBUG5, "datalake_fdw: dataLakeExplainForeignScan ends on segment: %d", DATALAKE_SEGMENT_ID);
}

/*
 * BeginForeignScan
 *   called during executor startup. perform any initialization
 *   needed, but not start the actual scan.
 */
static void
dataLakeBeginForeignScan(ForeignScanState *node, int eflags)
{
	elog(DEBUG5, "datalake_fdw: dataLakeBeginForeignScan starts on segment: %d", DATALAKE_SEGMENT_ID);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	dataLakeFdwScanState *dataLakesstate  	= (dataLakeFdwScanState *) palloc0(sizeof(dataLakeFdwScanState));
	Oid			foreigntableid 				= RelationGetRelid(node->ss.ss_currentRelation);
	dataLakesstate->options 				= getOptions(foreigntableid);
	dataLakesstate->rel						= node->ss.ss_currentRelation;
	List* fragmentData						= NIL;
	ForeignScan *foreignScan      			= (ForeignScan *) node->ss.ps.plan;
	dataLakesstate->provider = NULL;
	if (gp_external_enable_filter_pushdown)
		dataLakesstate->quals = node->ss.ps.plan->qual;

	if (eflags & EXEC_FLAG_VECTOR)
	{
		dataLakesstate->options->vectorization = true;
		checkValidRecordBatchOpt(dataLakesstate->options);
	}

	List *retrieved_attrs = (List *) list_nth(foreignScan->fdw_private, FdwScanPrivateRetrievedAttrs);
	/*
	 * When queried at all of the nodes. Need to go to object storage
	 * do list operation get file list. The file list is sent to the
	 * segment for scheduling
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		fragmentData = GetExternalFragmentList(dataLakesstate->rel, dataLakesstate->quals, dataLakesstate->options, NULL);
		foreignScan->fdw_private = list_concat(foreignScan->fdw_private, fragmentData);
		/* master does not process any fragments */
		return;
	}

	fragmentData = deserializeExternalFragmentList(dataLakesstate->rel, dataLakesstate->quals, dataLakesstate->options, foreignScan->fdw_private);

	dataLakesstate->options->readFdw = true;
	dataLakesstate->provider = initProvider(dataLakesstate->options->format, dataLakesstate->options->readFdw, dataLakesstate->options->vectorization);
	dataLakesstate->rel = node->ss.ss_currentRelation;
	dataLakesstate->fragments = fragmentData;
	dataLakesstate->retrieved_attrs = retrieved_attrs;

	if (hasZeorSelectedPartition(dataLakesstate))
	{
		node->fdw_state = (void*)dataLakesstate;
		return;
	}

	initScanStatue(dataLakesstate);

	node->fdw_state = (void*)dataLakesstate;

	elog(DEBUG5, "datalake_fdw: dataLakeBeginForeignScan ends on segment: %d", DATALAKE_SEGMENT_ID);
}

/*
 * IterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 *   Fetch one row from the foreign source, returning it in a tuple table slot
 *    (the node's ScanTupleSlot should be used for this purpose).
 *  Return NULL if no more rows are available.
 */
static TupleTableSlot *
dataLakeIterateForeignScan(ForeignScanState *node)
{
	elog(DEBUG5, "datalake_fdw: dataLakeIterateForeignScan Executing on segment: %d", DATALAKE_SEGMENT_ID);
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)node->fdw_state;

	if (hasZeorSelectedPartition(dataLakesstate))
	{
		TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
		ExecClearTuple(slot);
		return slot;
	}

	if (dataLakesstate->options->vectorization)
	{
		VirtualTupleTableSlot *vslot = (VirtualTupleTableSlot*)node->ss.ss_ScanTupleSlot;
		ExecClearTuple(&vslot->base);
		iterateRecordBatch(dataLakesstate, vslot);
		return (TupleTableSlot*)vslot;
	}
	else
	{
		TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
		//TODO error context
		// ErrorContextCallback errcallback;
		/*
		* The protocol for loading a virtual tuple into a slot is first
		* ExecClearTuple, then fill the values/isnull arrays, then
		* ExecStoreVirtualTuple.  If we don't find another row in the file, we
		* just skip the last step, leaving the slot empty as required.
		*
		* We can pass ExprContext = NULL because we read all columns from the
		* file, so no need to evaluate default expressions.
		*/
		ExecClearTuple(slot);

		iterateScanStatus(dataLakesstate, slot);
		return slot;
	}
}

/*
 * hasZeorSelectedPartition
 * If the hivePartitionConstraints condition is a null then
 * there is no partition key table.
 */
bool hasZeorSelectedPartition(dataLakeFdwScanState *dataLakesstate)
{
	if (SUPPORT_PARTITION_TABLE(dataLakesstate->options->gopher->gopherType,
			dataLakesstate->options->format,
			dataLakesstate->options->hiveOption->hivePartitionKey,
			dataLakesstate->options->hiveOption->datasource) &&
		!dataLakesstate->options->hiveOption->hivePartitionConstraints)
	{
		return true;
	}
	return false;
}

/*
 * ReScanForeignScan
 *		Restart the scan from the beginning
 */
static void
dataLakeReScanForeignScan(ForeignScanState *node)
{
	elog(DEBUG5, "datalake_fdw: dataLakeReScanForeignScan starts on segment: %d", DATALAKE_SEGMENT_ID);
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)node->fdw_state;
	endScanStatus(dataLakesstate);
	initScanStatue(dataLakesstate);
	elog(DEBUG5, "datalake_fdw: dataLakeReScanForeignScan ends on segment: %d", DATALAKE_SEGMENT_ID);
}

/*
 * EndForeignScan
 *		End the scan and release resources.
 */
static void
dataLakeEndForeignScan(ForeignScanState *node)
{
	elog(DEBUG5, "datalake_fdw: dataLakeEndForeignScan starts on segment: %d", DATALAKE_SEGMENT_ID);

	/* Do nothing in EXPLAIN (no ANALYZE) case. */
	if (node->fdw_state == NULL)
	{
		return;
	}

	ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;

	dataLakeFdwScanState *sstate = (dataLakeFdwScanState*)node->fdw_state;

	/* Release resources */
	if (foreignScan->fdw_private)
	{
		elog(DEBUG5, "Freeing fdw_private");
		freeFdwPrivate(sstate, foreignScan);
		foreignScan->fdw_private = NULL;
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		return;
	}

	endScanStatus(sstate);

	elog(DEBUG5, "datalake_fdw: dataLakeEndForeignScan ends on segment: %d", DATALAKE_SEGMENT_ID);
}


/*
 * dataLakeBeginForeignModify
 *		Begin an insert operation on a foreign table
 */
static void
dataLakeBeginForeignModify(ModifyTableState *mtstate,
					  ResultRelInfo *resultRelInfo,
					  List *fdw_private,
					  int subplan_index,
					  int eflags)
{
	elog(DEBUG5, "datalake_fdw: dataLakeBeginForeignModify starts on segment: %d", DATALAKE_SEGMENT_ID);

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	dataLakeFdwScanState *dataLakesstate  = (dataLakeFdwScanState *) palloc0(sizeof(dataLakeFdwScanState));
	Relation	relation 			= resultRelInfo->ri_RelationDesc;
	dataLakesstate->options 		= getOptions(RelationGetRelid(relation));
	dataLakesstate->rel				= relation;
	dataLakesstate->options->readFdw = false;
	resultRelInfo->ri_FdwState = dataLakesstate;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		// master
		return;
	}
	initModify(dataLakesstate);

	elog(DEBUG5, "datalake_fdw: dataLakeBeginForeignModify ends on segment: %d", DATALAKE_SEGMENT_ID);
}

/*
 * dataLakeExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
dataLakeExecForeignInsert(EState *estate,
					 ResultRelInfo *resultRelInfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot)
{
	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignInsert starts on segment: %d", DATALAKE_SEGMENT_ID);

	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)resultRelInfo->ri_FdwState;

	insertModify(dataLakesstate, slot);

	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignInsert ends on segment: %d", DATALAKE_SEGMENT_ID);
	return slot;
}

/*
 * dataLakeEndForeignModify
 *		Finish an insert operation on a foreign table
 */
static void
dataLakeEndForeignModify(EState *estate,
					ResultRelInfo *resultRelInfo)
{
	elog(DEBUG5, "datalake_fdw: dataLakeEndForeignModify starts on segment: %d", DATALAKE_SEGMENT_ID);

	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)resultRelInfo->ri_FdwState;
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		// master
		if (dataLakesstate)
		{
			pfree(dataLakesstate);
		}
		return;
	}
	else
	{
		endModify(dataLakesstate);
	}

	elog(DEBUG5, "datalake_fdw: dataLakeEndForeignModify ends on segment: %d", DATALAKE_SEGMENT_ID);
}

/*
 * dataLakeIsForeignRelUpdatable
 *  Assume table is updatable regardless of settings.
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
dataLakeIsForeignRelUpdatable(Relation rel)
{
	elog(DEBUG5, "datalake_fdw: dataLakeIsForeignRelUpdatable starts on segment: %d", DATALAKE_SEGMENT_ID);
	elog(DEBUG5, "datalake_fdw: dataLakeIsForeignRelUpdatable ends on segment: %d", DATALAKE_SEGMENT_ID);
	/* Only INSERTs are allowed at the moment */
	return 1u << (unsigned int) CMD_INSERT | 0u << (unsigned int) CMD_UPDATE | 0u << (unsigned int) CMD_DELETE;
}

static bool dataLakeIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
							  RangeTblEntry *rte)
{
	return false;
}

/*
 * Initiates a copy state for datalakeBeginForeignScan() and datalakeReScanForeignScan()
 */
static void
InitCopyState(dataLakeFdwScanState *sstate)
{
#if (PG_VERSION_NUM < 140000)
	CopyState	cstate;
#else
	CopyFromState	cstate;
#endif

	List* copy_options = getCopyOptions(RelationGetRelid(sstate->rel));
	/*
	 * Create CopyState from FDW options.  We always acquire all columns, so
	 * as to match the expected ScanTupleSlot signature.
	 */
	cstate = BeginCopyFrom(NULL,
						   sstate->rel,
#if (PG_VERSION_NUM >= 140000)
						   NULL,
#endif
						   NULL,
						   false,	/* is_program */
						   &CopyRead,	/* data_source_cb */
						   sstate,	/* data_source_cb_extra */
						   NIL, /* attnamelist */
						   copy_options);	/* copy options */

	cstate->cdbsreh = NULL; /* no SREH */
	cstate->errMode = ALL_OR_NOTHING;

	/* and 'fe_mgbuf' */
	cstate->fe_msgbuf = makeStringInfo();

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype input or output routines, and should be faster than retail
	 * pfree's anyway.
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
												"DatalakeFdwMemCxt",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	sstate->cstate.cstate_scan = cstate;
}

int
CopyRead(void *outbuf, int minlen, int maxlen, void *extra)
{
	dataLakeFdwScanState *sstate = (dataLakeFdwScanState*)extra;
	size_t n = 0;
	n = readBufferFromProvider(sstate->provider, outbuf, maxlen);
	return n;
}

void
initScanStatue(dataLakeFdwScanState *dataLakesstate)
{
	dataLakesstate->initcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "datalakeFdwMemScanInitCxt",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	createHandler(dataLakesstate->provider, (void*)dataLakesstate);

	if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
		FORMAT_IS_TEXT(dataLakesstate->options->format))
	{
		/* csv/text used copy */
		InitCopyState(dataLakesstate);
	}
	else
	{
		dataLakesstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "datalakeFdwMemCxt",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	}
}

void
iterateRecordBatch(dataLakeFdwScanState *dataLakesstate, VirtualTupleTableSlot *vslot)
{
	MemoryContextReset(dataLakesstate->rowcontext);
	MemoryContext oldContext = MemoryContextSwitchTo(dataLakesstate->rowcontext);
	bool found = readRecordBatch(dataLakesstate->provider, (void**)(&vslot->data));
	if (found)
	{
		ExecStoreVirtualTuple(&vslot->base);
	}
	MemoryContextSwitchTo(oldContext);
}

void
iterateScanStatus(dataLakeFdwScanState *dataLakesstate, TupleTableSlot *slot)
{
	bool found = false;
	if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
	FORMAT_IS_TEXT(dataLakesstate->options->format))
	{
		found = NextCopyFrom(dataLakesstate->cstate.cstate_scan,
						NULL,
						slot->tts_values,
						slot->tts_isnull);
	}
	else
	{
		MemoryContextReset(dataLakesstate->rowcontext);
		MemoryContext oldContext = MemoryContextSwitchTo(dataLakesstate->rowcontext);

		found = readFromProvider(dataLakesstate->provider, (void*)slot->tts_values, (void*)slot->tts_isnull);

		MemoryContextSwitchTo(oldContext);
	}

	if (found)
	{
		if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
			FORMAT_IS_TEXT(dataLakesstate->options->format))
		{
			if (dataLakesstate->cstate.cstate_scan->cdbsreh)
			{
				dataLakesstate->cstate.cstate_scan->cdbsreh->processed++;
			}
		}

		ExecStoreVirtualTuple(slot);
	}
}

void
endScanStatus(dataLakeFdwScanState *dataLakesstate)
{
	if (dataLakesstate->cstate.cstate_scan != NULL && (FORMAT_IS_CSV(dataLakesstate->options->format) ||
			FORMAT_IS_TEXT(dataLakesstate->options->format)))
	{
#if (PG_VERSION_NUM < 140000)
		EndCopyFrom(dataLakesstate->cstate.cstate_scan);
#else
		EndCopyScan(dataLakesstate->cstate.cstate_scan);
#endif
	}

	if (dataLakesstate->provider != NULL && Gp_role != GP_ROLE_DISPATCH)
	{
		destroyHandler(dataLakesstate->provider);
	}
	freeDataLakeOptions(dataLakesstate->options);
}

void
freeFdwPrivateList(List *fdw_private)
{
	List *fragmentLists = (List *) list_nth(fdw_private, FdwScanPrivateFragmentList);
	ListCell *cell;
	foreach(cell, fragmentLists)
	{
		List *fragment = (List*)lfirst(cell);
		char* filePath = strVal(list_nth(fragment, 0));
		if (filePath)
		{
			pfree(filePath);
		}
		char* length = strVal(list_nth(fragment, 1));
		if (length)
		{
			pfree(length);
		}
	}
	list_free_deep(fragmentLists);

	pfree(fdw_private);
}

void
freeFdwPrivatePartitionList(List *fdw_private)
{
	List *partitionData = list_nth(fdw_private, PrivatePartitionData);

	freePartitionList(partitionData);

	for (int i = PrivatePartitionFragmentLists; i < list_length(fdw_private); i++)
	{
		List* serializedFragmentList = (List *) list_nth(fdw_private, i);
		ListCell *cell;
		foreach(cell, serializedFragmentList)
		{
			List *fragment = (List*)lfirst(cell);
			char* filePath = strVal(list_nth(fragment, 0));
			if (filePath)
			{
				pfree(filePath);
			}
			char* length = strVal(list_nth(fragment, 1));
			if (length)
			{
				pfree(length);
			}
		}
		list_free_deep(serializedFragmentList);
	}
	pfree(fdw_private);
}

void
freeFdwPrivate(dataLakeFdwScanState *sstate, ForeignScan *foreignScan)
{
	if (foreignScan->fdw_private)
	{
		if (sstate != NULL)
		{
			if (SUPPORT_PARTITION_TABLE(sstate->options->gopher->gopherType,
			sstate->options->format,
			sstate->options->hiveOption->hivePartitionKey,
			sstate->options->hiveOption->datasource))
			{
				freeFdwPrivatePartitionList(foreignScan->fdw_private);
			}
			else
			{
				freeFdwPrivateList(foreignScan->fdw_private);
				foreignScan->fdw_private = NULL;
			}
		}
		else
		{
			int private_count = list_length(foreignScan->fdw_private);
			if (private_count > PrivatePartitionFragmentLists)
			{
				freeFdwPrivatePartitionList(foreignScan->fdw_private);
			}
			else
			{
				freeFdwPrivateList(foreignScan->fdw_private);
				foreignScan->fdw_private = NULL;
			}
		}
	}
}

static void
initModify(dataLakeFdwScanState *dataLakesstate)
{
	dataLakesstate->provider = initProvider(dataLakesstate->options->format, dataLakesstate->options->readFdw, dataLakesstate->options->vectorization);
	void *sstate = (void*)dataLakesstate;
	createHandler(dataLakesstate->provider, sstate);

	if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
		FORMAT_IS_TEXT(dataLakesstate->options->format))
	{
		initCopyStateForModify(dataLakesstate);
	}
	else
	{
		dataLakesstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											"datalakeFdwMemCxt",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);
	}
}

static void
initCopyStateForModify(dataLakeFdwScanState *dataLakesstate)
{
	List	   *copy_options;
#if (PG_VERSION_NUM < 140000)
	CopyState	cstate;
#else
	CopyToState	cstate;
#endif

	copy_options = getCopyOptions(RelationGetRelid(dataLakesstate->rel));

#if (PG_VERSION_NUM < 140000)
	cstate = BeginCopyTo(dataLakesstate->rel, copy_options);
#else
	cstate = BeginCopyToModify(dataLakesstate->rel, copy_options);
#endif

	/* Initialize 'out_functions', like CopyTo() would. */

	TupleDesc	tupDesc = RelationGetDescr(cstate->rel);
	Form_pg_attribute attr = tupDesc->attrs;
	int			num_phys_attrs = tupDesc->natts;

	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	ListCell   *cur;

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;

		getTypeOutputInfo(attr[attnum - 1].atttypid,
						  &out_func_oid,
						  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/* and 'fe_mgbuf' */
	cstate->fe_msgbuf = makeStringInfo();

	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
										"datalakeFdwMemCxt",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	dataLakesstate->cstate.cstate_modify = cstate;
}

/*
 * Set up CopyState for writing to an foreign table.
 */
#if (PG_VERSION_NUM < 140000)
static CopyState
BeginCopyTo(Relation forrel, List *options)
#else
static CopyToState
BeginCopyToModify(Relation forrel, List *options)
#endif
{
#if (PG_VERSION_NUM < 140000)
	CopyState	cstate;
#else
	CopyToState	cstate;
#endif

	Assert(forrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);

#if (PG_VERSION_NUM <= 90500)
	cstate = BeginCopy(false, forrel, NULL, NULL, NIL, options, NULL);
#elif (PG_VERSION_NUM < 120000)
	cstate = BeginCopy(false, forrel, NULL, NULL, forrel->rd_id, NIL, options, NULL);
#elif (PG_VERSION_NUM < 140000)
	cstate = BeginCopy(NULL, false, forrel, NULL, forrel->rd_id, NIL, options, NULL);
#else
	cstate = BeginCopy(NULL, forrel, NULL, forrel->rd_id, NIL, options, NULL);
#endif
	cstate->dispatch_mode = COPY_DIRECT;

	/*
	 * We use COPY_CALLBACK to mean that the each line should be left in
	 * fe_msgbuf. There is no actual callback!
	 */
	cstate->copy_dest = COPY_CALLBACK;

	/*
	 * Some more initialization, that in the normal COPY TO codepath, is done
	 * in CopyTo() itself.
	 */
#if (PG_VERSION_NUM < 140000)
	cstate->null_print_client = cstate->null_print; /* default */
	if (cstate->need_transcoding)
		cstate->null_print_client = pg_server_to_custom(cstate->null_print,
														cstate->null_print_len,
														cstate->file_encoding,
														cstate->enc_conversion_proc);
#else
	cstate->opts.null_print_client = cstate->opts.null_print; /* default */
	if (cstate->need_transcoding)
		cstate->opts.null_print_client = pg_server_to_any(cstate->opts.null_print,
														  cstate->opts.null_print_len,
														  cstate->opts.file_encoding);
#endif
	return cstate;
}

static void
insertModify(dataLakeFdwScanState *dataLakesstate, TupleTableSlot *slot)
{
	if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
			FORMAT_IS_TEXT(dataLakesstate->options->format))
	{

#if (PG_VERSION_NUM < 140000)
		CopyState	cstate = dataLakesstate->cstate.cstate_modify;
#else
		CopyToState	cstate = dataLakesstate->cstate.cstate_modify;
#endif
		/* TEXT or CSV */
		slot_getallattrs(slot);
		CopyOneRowTo(cstate, slot);
		CopySendEndOfRow(cstate);

		StringInfo	fe_msgbuf = cstate->fe_msgbuf;
		writeToProvider(dataLakesstate->provider, fe_msgbuf->data, fe_msgbuf->len);

		/* Reset our buffer to start clean next round */
		cstate->fe_msgbuf->len = 0;
		cstate->fe_msgbuf->data[0] = '\0';
	}
	else
	{
		MemoryContext oldcontext;
		MemoryContextReset(dataLakesstate->rowcontext);
		oldcontext = MemoryContextSwitchTo(dataLakesstate->rowcontext);

		slot_getallattrs(slot);
		writeToProvider(dataLakesstate->provider, slot, 0);

		MemoryContextSwitchTo(oldcontext);
	}
}

static void
endModify(dataLakeFdwScanState *dataLakesstate)
{
if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
			FORMAT_IS_TEXT(dataLakesstate->options->format))
	{
#if (PG_VERSION_NUM < 140000)
		EndCopyFrom(dataLakesstate->cstate.cstate_modify);
#else
		EndCopyModify(dataLakesstate->cstate.cstate_modify);
#endif
	}

	destroyHandler(dataLakesstate->provider);
	if (dataLakesstate)
	{
		freeDataLakeOptions(dataLakesstate->options);
		pfree(dataLakesstate);
	}
}


#if (PG_VERSION_NUM >= 140000)
/*
 * Clean up storage and release resources for COPY FROM.
 */
static void
EndCopyScan(CopyFromState cstate)
{
	/* No COPY FROM related resources except memory. */
	Assert(!cstate->is_program);
	Assert(cstate->filename == NULL);

	/* Clean up single row error handling related memory */
	if (cstate->cdbsreh)
		destroyCdbSreh(cstate->cdbsreh);

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Clean up storage and release resources for COPY TO.
 */
static void
EndCopyModify(CopyToState cstate)
{
	/* No COPY FROM related resources except memory. */
	Assert(!cstate->is_program);
	Assert(cstate->filename == NULL);

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}
#endif

static List *latestFragmentData = NIL;

static void
costDataLakeScan(ForeignPath *path, PlannerInfo *root,
				 RelOptInfo *baserel, ParamPathInfo *param_info)
{
	Cost		startup_cost = 0;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	/* Mark the path with the correct row estimate */
	if (param_info)
		path->path.rows = param_info->ppi_rows;
	else
		path->path.rows = baserel->rows;

	/*
	 * disk costs
	 */
	run_cost += seq_page_cost * baserel->pages;

	/* CPU costs */
	startup_cost += baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * baserel->tuples;

	path->path.startup_cost = startup_cost;
	path->path.total_cost = startup_cost + run_cost;
}

static ForeignScanState *
dataLakeAnalyzeBeginScan(Relation relation)
{
	int   i;
	List *fragmentData;
	List *retrieved_attrs = NIL;
	TupleDesc tupdesc = RelationGetDescr(relation);
	ForeignScanState *node = (ForeignScanState *) palloc0(sizeof(ForeignScanState));
	dataLakeFdwScanState *state = (dataLakeFdwScanState *) palloc0(sizeof(dataLakeFdwScanState));

	state->options = getOptions(RelationGetRelid(relation));

	fragmentData = deserializeExternalFragmentList(relation,
												   NIL,
												   state->options,
												   latestFragmentData);

	state->options->readFdw = true;
	state->provider = initProvider(state->options->format, state->options->readFdw, false);
	state->rel = relation;
	state->fragments = fragmentData;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		if (attr->attisdropped)
			continue;

		retrieved_attrs = lappend_int(retrieved_attrs, i);
	}

	state->retrieved_attrs = retrieved_attrs;

	node->ss.ps.plan = palloc0(sizeof(ForeignScan));
	node->ss.ss_ScanTupleSlot = table_slot_create(relation, NULL);
	node->fdw_state = state;

	if (hasZeorSelectedPartition(state))
		return node;

	initScanStatue(state);
	return node;
}

static TupleTableSlot *
dataLakeAnalyzeScanNext(ForeignScanState *node)
{
	return dataLakeIterateForeignScan(node);
}

static void
dataLakeAnalyzeEndScan(ForeignScanState *node)
{
	ExecDropSingleTupleTableSlot(node->ss.ss_ScanTupleSlot);
	dataLakeEndForeignScan(node);
}

static const char base64_[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static const int8 b64lookup_[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
	-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
	15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
	-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
};

static unsigned
pg_base64_encode(const char *src, unsigned len, char *dst)
{
	char	   *p,
			   *lend = dst + 76;
	const char *s,
			   *end = src + len;
	int			pos = 2;
	uint32		buf = 0;

	s = src;
	p = dst;

	while (s < end)
	{
		buf |= (unsigned char) *s << (pos << 3);
		pos--;
		s++;

		/* write it out */
		if (pos < 0)
		{
			*p++ = base64_[(buf >> 18) & 0x3f];
			*p++ = base64_[(buf >> 12) & 0x3f];
			*p++ = base64_[(buf >> 6) & 0x3f];
			*p++ = base64_[buf & 0x3f];

			pos = 2;
			buf = 0;
		}
		if (p >= lend)
		{
			*p++ = '\n';
			lend = p + 76;
		}
	}
	if (pos != 2)
	{
		*p++ = base64_[(buf >> 18) & 0x3f];
		*p++ = base64_[(buf >> 12) & 0x3f];
		*p++ = (pos == 0) ? base64_[(buf >> 6) & 0x3f] : '=';
		*p++ = '=';
	}

	return p - dst;
}

static unsigned
pg_base64_decode(const char *src, unsigned len, char *dst)
{
	const char *srcend = src + len,
			   *s = src;
	char	   *p = dst;
	char		c;
	int			b = 0;
	uint32		buf = 0;
	int			pos = 0,
				end = 0;

	while (s < srcend)
	{
		c = *s++;

		if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
			continue;

		if (c == '=')
		{
			/* end sequence */
			if (!end)
			{
				if (pos == 2)
					end = 1;
				else if (pos == 3)
					end = 2;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("unexpected \"=\"")));
			}
			b = 0;
		}
		else
		{
			b = -1;
			if (c > 0 && c < 127)
				b = b64lookup_[(unsigned char) c];
			if (b < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid symbol")));
		}
		/* add it to buffer */
		buf = (buf << 6) + b;
		pos++;
		if (pos == 4)
		{
			*p++ = (buf >> 16) & 255;
			if (end == 0 || end > 1)
				*p++ = (buf >> 8) & 255;
			if (end == 0 || end > 2)
				*p++ = buf & 255;
			buf = 0;
			pos = 0;
		}
	}

	if (pos != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid end sequence")));

	return p - dst;
}


static unsigned
pg_base64_enc_len(const char *src, unsigned srclen)
{
	/* 3 bytes will be converted to 4, linefeed after 76 chars */
	return (srclen + 2) * 4 / 3 + srclen / (76 * 3 / 4);
}

static unsigned
pg_base64_dec_len(const char *src, unsigned srclen)
{
	return (srclen * 3) >> 2;
}

static char *
encode_string(char *data, unsigned len)
{
	char   *result;
	int		res;
	int		resultlen;

	resultlen = pg_base64_enc_len(data, len);
	result = palloc(resultlen + 1);
	res = pg_base64_encode(data, len, result);
	if (res > resultlen)
		elog(FATAL, "overflow - base64 encode estimate too small");

	result[res] = '\0';
	return result;
}

static char *
decode_string(char *data, unsigned len, int *decodedLen)
{
	char   *result;
	int		res;
	int		resultlen;

	resultlen = pg_base64_dec_len(data, len);
	result = palloc(resultlen);
	res = pg_base64_decode(data, len, result);
	if (res > resultlen)
		elog(FATAL, "overflow - base64 decode estimate too small");

	*decodedLen = res;
	return result;
}

static int
process_sample_rows(Portal portal,
					QueryDesc  *queryDesc,
					Relation onerel,
					HeapTuple *rows,
					int targrows,
					double *totalrows,
					double *totaldeadrows)
{
	/*
	 * 'colLargeRowIndexes' is essentially an argument, but it's passed via a
	 * global variable to avoid changing the AcquireSampleRowsFunc prototype.
	 */
	Bitmapset **colLargeRowIndexes = acquire_func_colLargeRowIndexes;
	/* double     *colLargeRowLength = acquire_func_colLargeRowLength; */
	TupleDesc	relDesc = RelationGetDescr(onerel);
	TupleDesc	funcTupleDesc;
	TupleDesc	sampleTupleDesc;
	int			sampleTuples;	/* 32 bit - assume that number of tuples will not > 2B */
	Datum	   *funcRetValues;
	bool	   *funcRetNulls;
	int			ncolumns;
	AttInMetadata *attinmeta;
	int			numLiveColumns;
	int			i;
	int			index = 0;
	TupleTableSlot *slot;
	Datum	   *dvalues;
	bool	   *dnulls;

	/*
	 * Count the number of columns, excluding dropped columns. We'll need that
	 * later.
	 */
	numLiveColumns = 0;
	for (i = 0; i < relDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(relDesc, i);

		if (attr->attisdropped)
			continue;

		numLiveColumns++;
	}

	/*
	 * Build a modified tuple descriptor for the table.
	 *
	 * Some datatypes need special treatment, so we cannot use the relation's
	 * original tupledesc.
	 *
	 * Also create tupledesc of return record of function gp_acquire_sample_rows.
	 */
	sampleTupleDesc = CreateTupleDescCopy(relDesc);
	ncolumns = numLiveColumns + FIX_ATTR_NUM;
	
	funcTupleDesc = CreateTemplateTupleDesc(ncolumns);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 1, "", FLOAT8OID, -1, 0);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 2, "", FLOAT8OID, -1, 0);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 3, "", FLOAT8ARRAYOID, -1, 0);

	for (i = 0; i < relDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(relDesc, i);

		Oid			typid = gp_acquire_sample_rows_col_type(attr->atttypid);

		TupleDescAttr(sampleTupleDesc, i)->atttypid = typid;

		if (!attr->attisdropped)
		{
			TupleDescInitEntry(funcTupleDesc, (AttrNumber) 4 + index, "",
							   typid, attr->atttypmod, attr->attndims);

			index++;
		}
	}

	/* For RECORD results, make sure a typmod has been assigned */
	Assert(funcTupleDesc->tdtypeid == RECORDOID && funcTupleDesc->tdtypmod < 0);
	assign_record_type_typmod(funcTupleDesc);

	attinmeta = TupleDescGetAttInMetadata(sampleTupleDesc);

	/*
	 * Read the result set from each segment. Gather the sample rows *rows,
	 * and sum up the summary rows for grand 'totalrows' and 'totaldeadrows'.
	 */
	funcRetValues = (Datum *) palloc0(funcTupleDesc->natts * sizeof(Datum));
	funcRetNulls = (bool *) palloc0(funcTupleDesc->natts * sizeof(bool));
	dvalues = (Datum *) palloc0(relDesc->natts * sizeof(Datum));
	dnulls = (bool *) palloc0(relDesc->natts * sizeof(bool));
	sampleTuples = 0;
	*totalrows = 0;
	*totaldeadrows = 0;

	slot = MakeSingleTupleTableSlot(queryDesc->tupDesc, &TTSOpsMinimalTuple);

	for (;;)
	{
		bool		ok;
		TupleDesc	typeinfo;
		int			natts;
		Datum		attr;
		bool		isnull;
		double		this_totalrows = 0;
		double		this_totaldeadrows = 0;

		CHECK_FOR_INTERRUPTS();

		ok = tuplestore_gettupleslot(portal->holdStore, true, false, slot);

		if (!ok)
			break;

		typeinfo = slot->tts_tupleDescriptor;
		natts = typeinfo->natts;

		/* There should be only one attribute with OID RECORDOID. */
		if (1 != natts)
		{
			elog(ERROR,
				"wrong number of attributes %d when 1 expected",
				natts);
		}

		if (RECORDOID != typeinfo->attrs[0].atttypid)
		{
			elog(ERROR,
				"wrong attribute OID %d, RECORDOID %d is expected",
				typeinfo->attrs[0].atttypid, RECORDOID);

		}

		/* Make sure the tuple is fully deconstructed */
		slot_getallattrs(slot);

		/* There should be only one attribute with OID RECORDOID */
		attr = slot_getattr(slot, 1, &isnull);
		if (isnull)
		{
			elog(ERROR,
				"null value for attribute in tuple");
		}

		/* Get record from attribute and parse it */
		{
			HeapTupleHeader rec = (HeapTupleHeader) PG_DETOAST_DATUM(attr);
			Oid			tupType;
			int32		tupTypmod;
			TupleDesc	tupdesc;
			HeapTupleData tuple;

			/* Extract type info from the tuple itself */
			tupType = HeapTupleHeaderGetTypeId(rec);
			tupTypmod = HeapTupleHeaderGetTypMod(rec);
			tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

			/* Build a temporary HeapTuple control structure */
			tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
			ItemPointerSetInvalid(&(tuple.t_self));
			tuple.t_data = rec;

			/* Break down the tuple into fields */
			heap_deform_tuple(&tuple, tupdesc, funcRetValues, funcRetNulls);

			if (!funcRetNulls[0])
			{
				/* This is a summary row. */
				this_totalrows = DatumGetFloat8(funcRetValues[0]);
				this_totaldeadrows = DatumGetFloat8(funcRetValues[1]);
				(*totalrows) += this_totalrows;
				(*totaldeadrows) += this_totaldeadrows;
			}
			else
			{
				/* This is a sample row. */
				if (sampleTuples >= targrows)
					elog(ERROR, "too many sample rows received from gp_acquire_sample_rows");

				/* Read the 'toolarge' bitmap, if any */
				if (colLargeRowIndexes && !funcRetNulls[2])
				{
					ArrayType  *arrayVal;
					Datum	   *largelength;
					bool	   *nulls;
					int	    numelems;
					arrayVal = DatumGetArrayTypeP(funcRetValues[2]);
					deconstruct_array(arrayVal, FLOAT8OID, 8, true, 'd',
								&largelength, &nulls, &numelems);

					for (i = 0; i < relDesc->natts; i++)
					{
						Form_pg_attribute attr = TupleDescAttr(relDesc, i);

						if (attr->attisdropped)
							continue;

						if (largelength[i] != (Datum) 0)
						{
							colLargeRowIndexes[i] = bms_add_member(colLargeRowIndexes[i], sampleTuples);
							/* colLargeRowLength[i] += DatumGetFloat8(largelength[i]); */
						}
					}
				}

				/* Process the columns */
				index = 0;
				for (i = 0; i < relDesc->natts; i++)
				{
					Form_pg_attribute attr = TupleDescAttr(relDesc, i);

					if (attr->attisdropped)
					{
						dnulls[i] = true;
						continue;
					}

					dnulls[i] = funcRetNulls[FIX_ATTR_NUM + index];
					dvalues[i] = funcRetValues[FIX_ATTR_NUM + index];
					index++;	/* Move index to the next result set attribute */
				}

				/*
				* Form a tuple
				*/
				rows[sampleTuples] = heap_form_tuple(attinmeta->tupdesc,
													dvalues,
													dnulls);
				sampleTuples++;

				/*
				 * note: we don't set the OIDs in the sample. ANALYZE doesn't
				 * collect stats for them
				 */
			}
			ReleaseTupleDesc(tupdesc);
		}

		ExecClearTuple(slot);
	}
	ExecDropSingleTupleTableSlot(slot);
	pfree(funcRetValues);
	pfree(funcRetNulls);
	pfree(dvalues);
	pfree(dnulls);

	return sampleTuples;
}

Datum
datalake_acquire_sample_rows(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx = NULL;
	gp_acquire_sample_rows_context *ctx;
	MemoryContext oldcontext;
	Oid			relOid = PG_GETARG_OID(0);
	int32		targrows = PG_GETARG_INT32(1);
	bool        inherited = PG_GETARG_BOOL(2);
	text	   *encodedFragment = PG_GETARG_TEXT_PP(3);

	TupleDesc	relDesc;
	TupleDesc	outDesc;
	int			live_natts;

	if (targrows < 1)
		elog(ERROR, "invalid targrows argument");

	if (SRF_IS_FIRSTCALL())
	{
		Relation	onerel;
		int			attno;
		int			outattno;
		VacuumParams	params;
		RangeVar	   *this_rangevar;
		char *decodedFragment;
		int decodedLen;

		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function
		 * calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Construct the context to keep across calls. */
		ctx = (gp_acquire_sample_rows_context *) palloc0(sizeof(gp_acquire_sample_rows_context));
		ctx->targrows = targrows;
		ctx->inherited = inherited;

		if (!pg_class_ownercheck(relOid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
						   get_rel_name(relOid));

		onerel = table_open(relOid, AccessShareLock);
		relDesc = RelationGetDescr(onerel);

		decodedFragment = decode_string(VARDATA_ANY(encodedFragment), VARSIZE_ANY_EXHDR(encodedFragment), &decodedLen);
		latestFragmentData = (List *) deserializeNode(decodedFragment, decodedLen);
		pfree(decodedFragment);

		MemSet(&params, 0, sizeof(VacuumParams));
		params.options |= VACOPT_ANALYZE;
		params.freeze_min_age = -1;
		params.freeze_table_age = -1;
		params.multixact_freeze_min_age = -1;
		params.multixact_freeze_table_age = -1;
		params.is_wraparound = false;
		params.log_min_duration = -1;
		params.index_cleanup = VACOPTVALUE_AUTO;
		params.truncate = VACOPTVALUE_AUTO;

		this_rangevar = makeRangeVar(get_namespace_name(onerel->rd_rel->relnamespace),
									 pstrdup(RelationGetRelationName(onerel)),
									 -1);
		analyze_rel(relOid, this_rangevar, &params, NULL,
					true, GetAccessStrategy(BAS_VACUUM), ctx);

		/* Count the number of non-dropped cols */
		live_natts = 0;
		for (attno = 1; attno <= relDesc->natts; attno++)
		{
			Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);

			if (relatt->attisdropped)
				continue;
			live_natts++;
		}

		outDesc = CreateTemplateTupleDesc(NUM_SAMPLE_FIXED_COLS + live_natts);

		/* First, some special cols: */

		/* These two are only set in the last, summary row */
		TupleDescInitEntry(outDesc,
						   1,
						   "totalrows",
						   FLOAT8OID,
						   -1,
						   0);
		TupleDescInitEntry(outDesc,
						   2,
						   "totaldeadrows",
						   FLOAT8OID,
						   -1,
						   0);

		/* extra column to indicate oversize cols */
		TupleDescInitEntry(outDesc,
						   3,
						   "oversized_cols_length",
						   FLOAT8ARRAYOID,
						   -1,
						   0);

		outattno = NUM_SAMPLE_FIXED_COLS + 1;
		for (attno = 1; attno <= relDesc->natts; attno++)
		{
			Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);
			Oid			typid;

			if (relatt->attisdropped)
				continue;

			typid = gp_acquire_sample_rows_col_type(relatt->atttypid);

			TupleDescInitEntry(outDesc,
							   outattno++,
							   NameStr(relatt->attname),
							   typid,
							   relatt->atttypmod,
							   0);
		}

		BlessTupleDesc(outDesc);
		funcctx->tuple_desc = outDesc;

		ctx->onerel = onerel;
		funcctx->user_fctx = ctx;
		ctx->outDesc = outDesc;

		ctx->index = 0;
		ctx->summary_sent = false;
		/*
		 * we only get sample data from segindex 0 for replicated table
		 */
		if (Gp_role == GP_ROLE_EXECUTE && GpPolicyIsReplicated(onerel->rd_cdbpolicy)
									   && GpIdentity.segindex > 0)
		{
			ctx->index = ctx->num_sample_rows;
			ctx->summary_sent = true;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	ctx = funcctx->user_fctx;
	relDesc = RelationGetDescr(ctx->onerel);
	outDesc = ctx->outDesc;

	Datum	   *outvalues = (Datum *) palloc(outDesc->natts * sizeof(Datum));
	bool	   *outnulls = (bool *) palloc(outDesc->natts * sizeof(bool));
	HeapTuple	res;

	/* First return all the sample rows */
	if (ctx->index < ctx->num_sample_rows)
	{
		HeapTuple	relTuple = ctx->sample_rows[ctx->index];
		int			attno;
		int			outattno;
		bool			has_toolarge = false;
		Datum	   *relvalues = (Datum *) palloc(relDesc->natts * sizeof(Datum));
		bool	   *relnulls = (bool *) palloc(relDesc->natts * sizeof(bool));
		Datum      *oversized_cols_length = (Datum *) palloc0(relDesc->natts * sizeof(Datum));

		heap_deform_tuple(relTuple, relDesc, relvalues, relnulls);

		outattno = NUM_SAMPLE_FIXED_COLS + 1;
		for (attno = 1; attno <= relDesc->natts; attno++)
		{
			Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);
			Datum		relvalue;
			bool		relnull;

			if (relatt->attisdropped)
				continue;
			relvalue = relvalues[attno - 1];
			relnull = relnulls[attno - 1];

			/* Is this attribute "too large" to return? */
			if (relatt->attlen == -1 && !relnull)
			{
				Size		toasted_size = toast_datum_size(relvalue);

				if (toasted_size > WIDTH_THRESHOLD)
				{
					oversized_cols_length[attno - 1] = Float8GetDatum((double)toasted_size);
					has_toolarge = true;
					relvalue = (Datum) 0;
					relnull = true;
				}
			}
			outvalues[outattno - 1] = relvalue;
			outnulls[outattno - 1] = relnull;
			outattno++;
		}

		/*
		 * If any of the attributes were oversized, construct the text datum
		 * to represent the bitmap.
		 */
		if (has_toolarge)
		{
			outvalues[2] = PointerGetDatum(construct_array(oversized_cols_length, relDesc->natts,
														FLOAT8OID, 8, true, 'd'));
			outnulls[2] = false;
		}
		else
		{
			outvalues[2] = (Datum) 0;
			outnulls[2] = true;
		}
		outvalues[0] = (Datum) 0;
		outnulls[0] = true;
		outvalues[1] = (Datum) 0;
		outnulls[1] = true;

		res = heap_form_tuple(outDesc, outvalues, outnulls);

		ctx->index++;

		SIMPLE_FAULT_INJECTOR("returned_sample_row");

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(res));
	}
	else if (!ctx->summary_sent)
	{
		/* Done returning the sample. Return the summary row, and we're done. */
		int			outattno;

		outvalues[0] = Float8GetDatum(ctx->totalrows);
		outnulls[0] = false;
		outvalues[1] = Float8GetDatum(ctx->totaldeadrows);
		outnulls[1] = false;

		outvalues[2] = (Datum) 0;
		outnulls[2] = true;
		for (outattno = NUM_SAMPLE_FIXED_COLS + 1; outattno <= outDesc->natts; outattno++)
		{
			outvalues[outattno - 1] = (Datum) 0;
			outnulls[outattno - 1] = true;
		}

		res = heap_form_tuple(outDesc, outvalues, outnulls);

		ctx->summary_sent = true;

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(res));
	}

	table_close(ctx->onerel, AccessShareLock);

	pfree(ctx);
	funcctx->user_fctx = NULL;

	SRF_RETURN_DONE(funcctx);

}

/*
 * Build a querydesc for a sql, set "dest" to portal->holdStore
 */
static QueryDesc *build_querydesc(Portal portal, char *sql)
{
	List	   *raw_parsetree_list;
	RawStmt	   *parsetree;
	List	   *querytree_list;
	List	   *plantree_list;
	PlannedStmt *plan_stmt;
	DestReceiver *destReceiver;
	QueryDesc  *queryDesc = NULL;
	destReceiver = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(destReceiver,
									portal->holdStore,
									portal->holdContext,
									false,
									NULL,
									NULL);

	/*
	 * Parse the SQL string into a list of raw parse trees.
	 */
	raw_parsetree_list = pg_parse_query(sql);

	/*
	 * Do parse analysis, rule rewrite, planning, and execution for each raw
	 * parsetree.
	 */

	/* There is only one element in list due to simple select. */
	Assert(list_length(raw_parsetree_list) == 1);
	parsetree = (RawStmt *) linitial(raw_parsetree_list);

	querytree_list = pg_analyze_and_rewrite(parsetree,
											sql,
											NULL,
											0,
											NULL);
	plantree_list = pg_plan_queries(querytree_list, sql, 0, NULL);

	/* There is only one statement in list due to simple select. */
	Assert(list_length(plantree_list) == 1);
	plan_stmt = (PlannedStmt *) linitial(plantree_list);

	queryDesc = CreateQueryDesc(plan_stmt,
								sql,
								GetActiveSnapshot(),
								InvalidSnapshot,
								destReceiver,
								NULL,
								NULL,
								INSTRUMENT_NONE);



	list_free_deep(querytree_list);
	list_free_deep(raw_parsetree_list);

	return queryDesc;
}

static int
acquire_sample_rows_dispatcher(Relation relation, bool inh, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows, double *totaldeadrows)
{
	StringInfoData str;
	int			perseg_targrows;
	int			sampleTuples;	/* 32 bit - assume that number of tuples will not > 2B */
	char	   *sql;
	Portal		portal;
	dataLakeOptions *options;
	char *sFragment;
	char *encodedFragment;
	int sFragmentLen;
	int sFragmentUnCompressedLen;
	List *sFragmentList;
	QueryDesc  *queryDesc = NULL;

	Assert(targrows > 0.0);

	/*
	 * Step1: Construct SQL command to dispatch to segments.
	 *
	 * Acquire an evenly-sized sample from each segment.
	 *
	 * XXX: If there's a significant bias between the segments, i.e. some
	 * segments have a lot more rows than others, the sample will biased, too.
	 * Would be nice to improve that, but it's not clear how. We could issue
	 * another query to get the table size from each segment first, and use
	 * those to weigh the sample size to get from each segment. But that'd
	 * require an extra round-trip, which is also not good. The caller
	 * actually already did that, to get the total relation size, but it
	 * doesn't pass that down to us, let alone the per-segment sizes.
	 */
	perseg_targrows = targrows / relation->rd_cdbpolicy->numsegments;

	options = getOptions(RelationGetRelid(relation));

	sFragmentList = list_make2(makeString("dummy"), makeString("dummy"));
	sFragmentList = list_concat(sFragmentList, latestFragmentData);
	sFragment = serializeNode((Node *) sFragmentList, &sFragmentLen, &sFragmentUnCompressedLen);
	encodedFragment = encode_string(sFragment, sFragmentLen);
	pfree(sFragment);

	/*
	 * Did not use 'select * from pg_catalog.gp_acquire_sample_rows(...) as (..);'
	 * here. Because it requires to specify columns explicitly which leads to
	 * permission check on each columns. This is not consistent with GPDB5 and
	 * may result in different behaviour under different acl configuration.
	 */
	initStringInfo(&str);
	appendStringInfo(&str, "select public.datalake_acquire_sample_rows(%u, %d, '%s', '%s');",
					 RelationGetRelid(relation),
					 perseg_targrows,
					 inh ? "t" : "f",
					 encodedFragment);
	pfree(encodedFragment);

	/*
	 * Step2: Execute the constructed SQL.
	 *
	 * Do not use SPI here, because there might be a large number of wide rows
	 * returned and stored in memory, SPI cannot spill data to disk which may
	 * lead to OOM easily.
	 *
	 * Do not use SPI cusror either, because we should use SPI_cursor_fetch to fetch
	 * results in batches, which may have bad performance.
	 *
	 * Use ExecutorStart|ExecutorRun|ExecutorEnd to execute a plan and store results
	 * into tuplestore could handle this situation well.
	 *
	 * Execute the given query and store the results into portal->holdStore to
	 * avoid memory error.
	 */
	elog(elevel, "Executing SQL: %s", str.data);
	sql = str.data;
	/* Create a new portal to run the query in */
	portal = CreateNewPortal();
	/* Don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;
	/* use a tuplestore to store received tuples to avoid out of memory error */
	PortalCreateHoldStore(portal);
	queryDesc = build_querydesc(portal, sql);

	/* Call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* Run the plan  */
	ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

	/* Wait for completion of all qExec processes. */
	if (queryDesc->estate->dispatcherState
		&& queryDesc->estate->dispatcherState->primaryResults)
	{
		cdbdisp_checkDispatchResult(queryDesc->estate->dispatcherState, DISPATCH_WAIT_NONE);
	}

	ExecutorFinish(queryDesc);
	/*
	 * Step3: process results.
	 */
	sampleTuples = process_sample_rows(portal, queryDesc, relation, rows,
									targrows, totalrows, totaldeadrows);

	ExecutorEnd(queryDesc);
	FreeQueryDesc(queryDesc);
	PortalDrop(portal, false);

	return sampleTuples;
}

/*
 * Collect sample rows from the result of query.
 *	 - Use all tuples in sample until target # of samples are collected.
 *	 - Subsequently, replace already-sampled tuples randomly.
 */
static void
analyze_row_processor(TupleTableSlot *slot, DataLakeAnalyzeState *astate)
{
	int			targrows = astate->targrows;
	int			pos;			/* array index to store tuple in */
	MemoryContext oldcontext;

	/* Always increment sample row counter. */
	astate->samplerows += 1;

	/*
	 * Determine the slot where this sample row should be stored.  Set pos to
	 * negative value to indicate the row should be skipped.
	 */
	if (astate->numrows < targrows)
	{
		/* First targrows rows are always included into the sample */
		pos = astate->numrows++;
	}
	else
	{
		/*
		 * Now we start replacing tuples in the sample until we reach the end
		 * of the relation.  Same algorithm as in acquire_sample_rows in
		 * analyze.c; see Jeff Vitter's paper.
		 */
		if (astate->rowstoskip < 0)
			astate->rowstoskip = reservoir_get_next_S(&astate->rstate, astate->samplerows, targrows);

		if (astate->rowstoskip <= 0)
		{
			/* Choose a random reservoir element to replace. */
			pos = (int) (targrows * sampler_random_fract(astate->rstate.randstate));
			Assert(pos >= 0 && pos < targrows);
			heap_freetuple(astate->rows[pos]);
		}
		else
		{
			/* Skip this tuple. */
			pos = -1;
		}

		astate->rowstoskip -= 1;
	}

	if (pos >= 0)
	{
		/*
		 * Create sample tuple from current result row, and store it in the
		 * position determined above.  The tuple has to be created in anl_cxt.
		 */
		oldcontext = MemoryContextSwitchTo(astate->anl_cxt);

		astate->rows[pos] = ExecCopySlotHeapTuple(slot);

		MemoryContextSwitchTo(oldcontext);
	}
}


/*
 * Acquire a random sample of rows from foreign table managed by datalake-fdw.
 *
 * We fetch the whole table from the remote side and pick out some sample rows.
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the table and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the table.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
segmentAcquireSampleRowsFunc(Relation relation, int elevel,
							  HeapTuple *rows, int targRows,
							  double *totalRows,
							  double *totalDeadRows)
{
	DataLakeAnalyzeState astate;
	ForeignScanState *state;
	TupleTableSlot *slot;

	/* Initialize workspace state */
	astate.rows = rows;
	astate.targrows = targRows;
	astate.numrows = 0;
	astate.samplerows = 0;
	astate.rowstoskip = -1;		/* -1 means not set yet */
	reservoir_init_selection_state(&astate.rstate, targRows);

	/* Remember ANALYZE context, and create a per-tuple temp context */
	astate.anl_cxt = CurrentMemoryContext;
	astate.temp_cxt = AllocSetContextCreate(CurrentMemoryContext,
											"datalake_fdw temporary data",
											ALLOCSET_SMALL_SIZES);

	state = dataLakeAnalyzeBeginScan(relation);
	for (;;)
	{
		/* Allow users to cancel long query */
		CHECK_FOR_INTERRUPTS();

		slot = dataLakeAnalyzeScanNext(state);
		if (TupIsNull(slot))
			break;

		analyze_row_processor(slot, &astate);
	}

	dataLakeAnalyzeEndScan(state);

	/* We assume that we have no dead tuple. */
	*totalDeadRows = 0.0;

	/* We've retrieved all living tuples from foreign server. */
	*totalRows = astate.samplerows;

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": table contains %.0f rows, %d rows in sample",
					RelationGetRelationName(relation),
					astate.samplerows, astate.numrows)));

	return astate.numrows;
}


static int
dataLakeAcquireSampleRowsFunc(Relation relation, int elevel,
							  HeapTuple *rows, int targRows,
							  double *totalRows,
							  double *totalDeadRows)
{
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* Fetch sample from the segments. */
		return acquire_sample_rows_dispatcher(relation, false, elevel,
											  rows, targRows,
											  totalRows, totalDeadRows);
	}

	return segmentAcquireSampleRowsFunc(relation, elevel, rows, 
										targRows, totalRows, totalDeadRows);
}

/*
 * dataLakeAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
dataLakeAnalyzeForeignTable(Relation relation,
							AcquireSampleRowsFunc *func,
							BlockNumber *totalPages)
{
	int64_t totalSize = 0;
	dataLakeOptions *options;

	/* Return the row-analysis function pointer */
	*func = dataLakeAcquireSampleRowsFunc;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		options = getOptions(RelationGetRelid(relation));
		latestFragmentData = GetExternalFragmentList(relation, NULL, options, &totalSize);
	}

	/*
	 * Convert size to pages.  Must return at least 1 so that we can tell
	 * later on that pg_class.relpages is not default.
	 */
	*totalPages = (totalSize + (BLCKSZ - 1)) / BLCKSZ;
	if (*totalPages < 1)
		*totalPages = 1;

	return true;
}
