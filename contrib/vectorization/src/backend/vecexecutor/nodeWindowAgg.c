/*-------------------------------------------------------------------------
 *
 * nodeVecWindowAgg.c
 *	  routines to handle WindowAgg nodes.
 *
 * A WindowAgg node evaluates "window functions" across suitable partitions
 * of the input tuple set.  Any one WindowAgg works for just a single window
 * specification, though it can evaluate multiple window functions sharing
 * identical window specifications.  The input tuples are required to be
 * delivered in sorted order, with the PARTITION BY columns (if any) as
 * major sort keys and the ORDER BY columns (if any) as minor sort keys.
 * (The planner generates a stack of WindowAggs with intervening Sort nodes
 * as needed, if a query involves more than one window specification.)
 *
 * Since window functions can require access to any or all of the rows in
 * the current partition, we accumulate rows of the partition into a
 * tuplestore.  The window functions are called using the WindowObject API
 * so that they can access those rows as needed.
 *
 * We also support using plain aggregate functions as window functions.
 * For these, the regular Agg-node environment is emulated for each partition.
 * As required by the SQL spec, the output represents the value of the
 * aggregate function over all rows in the current row's window frame.
 *
 *
 * Portions Copyright (c) 2016-2020, HashData
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/nodeVecWindowAgg.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbvars.h" /* mpp_hybrid_hash_agg */
#include "executor/executor.h"
#include "executor/nodeWindowAgg.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/numeric.h"
#include "utils/regproc.h"

#include "cdb/cdbexplain.h"
#include "lib/stringinfo.h"             /* StringInfo */
#include "optimizer/walkers.h"
#include "cdb/cdbvars.h"


#include "utils/tuptable_vec.h"
#include "utils/guc_vec.h"
#include "utils/fmgr_vec.h"
#include "vecexecutor/execnodes.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/nodeWindowAgg.h"
#include "vecexecutor/execAmi.h"
/*
 * All the window function APIs are called with this object, which is passed
 * to window functions as fcinfo->context.
 */
typedef struct WindowObjectData
{
	NodeTag		type;
	WindowAggState *winstate;	/* parent WindowAggState */
	List	   *argstates;		/* ExprState trees for fn's arguments */
	void	   *localmem;		/* WinGetPartitionLocalMemory's chunk */
	int			markptr;		/* tuplestore mark pointer for this fn */
	int			readptr;		/* tuplestore read pointer for this fn */
	int64		markpos;		/* row that markptr is positioned on */
	int64		seekpos;		/* row that readptr is positioned on */
} WindowObjectData;

typedef struct WindowObjectData *WindowObject;

/*
 * We have one WindowStatePerFunc struct for each window function and
 * window aggregate handled by this node.
 */
typedef struct WindowStatePerFuncData
{
	/* Links to WindowFunc expr and state nodes this working state is for */
	WindowFuncExprState *wfuncstate;
	WindowFunc *wfunc;

	int			numArguments;	/* number of arguments */

	FmgrInfo	flinfo;			/* fmgr lookup data for window function */

	Oid			winCollation;	/* collation derived for window function */

	/*
	 * We need the len and byval info for the result of each function in order
	 * to know how to copy/delete values.
	 */
	int16		resulttypeLen;
	bool		resulttypeByVal;

	bool		plain_agg;		/* is it just a plain aggregate function? */
	int			aggno;			/* if so, index of its WindowStatePerAggData */

	WindowObject winobj;		/* object used in window function API */
}			WindowStatePerFuncData;

/*
 * For plain aggregate window functions, we also have one of these.
 */
typedef struct WindowStatePerAggData
{
	/* Oids of transition functions */
	Oid			transfn_oid;
	Oid			invtransfn_oid; /* may be InvalidOid */
	Oid			finalfn_oid;	/* may be InvalidOid */

	/*
	 * fmgr lookup data for transition functions --- only valid when
	 * corresponding oid is not InvalidOid.  Note in particular that fn_strict
	 * flags are kept here.
	 */
	FmgrInfo	transfn;
	FmgrInfo	invtransfn;
	FmgrInfo	finalfn;

	int			numFinalArgs;	/* number of arguments to pass to finalfn */

	/*
	 * initial value from pg_aggregate entry
	 */
	Datum		initValue;
	bool		initValueIsNull;

	/*
	 * cached value for current frame boundaries
	 */
	Datum		resultValue;
	bool		resultValueIsNull;

	/*
	 * Support for DISTINCT-qualified aggregates. For example:
	 *
	 * COUNT(DISTINCT foo) OVER (PARTITION BY bar)
	 *
	 * This is only supported for aggregates that take a single argument
	 * (we checked for that in parse analysis).
	 */
	bool		isDistinct;				/* is this a DISTINCT-qualified aggregate? */
	Oid			distinctType;			/* type of the argument */
	bool		distinctTypeByVal;
	Oid			distinctColl;
	/* support for sorting by the argument type */
	Oid			distinctLtOper;
	SortSupportData distinctComparator;

	/* Input values accumulated for this aggregate so far. */
	Tuplesortstate *distinctSortState;

	/*
	 * We need the len and byval info for the agg's input, result, and
	 * transition data types in order to know how to copy/delete values.
	 */
	int16		inputtypeLen,
				resulttypeLen,
				transtypeLen;
	bool		inputtypeByVal,
				resulttypeByVal,
				transtypeByVal;

	int			wfuncno;		/* index of associated WindowStatePerFuncData */

	/* Context holding transition value and possibly other subsidiary data */
	MemoryContext aggcontext;	/* may be private, or winstate->aggcontext */

	/* Current transition value */
	Datum		transValue;		/* current transition value */
	bool		transValueIsNull;

	int64		transValueCount;	/* number of currently-aggregated rows */

	/* Data local to eval_windowaggregates() */
	bool		restart;		/* need to restart this agg in this cycle? */
} WindowStatePerAggData;

static TupleTableSlot *ExecVecWindowAgg(PlanState *pstate);

static Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
{
	Oid			typinput,
				typioparam;
	char	   *strInitVal;
	Datum		initVal;

	getTypeInputInfo(transtype, &typinput, &typioparam);
	strInitVal = TextDatumGetCString(textInitVal);
	initVal = OidInputFunctionCall(typinput, strInitVal,
								   typioparam, -1);
	pfree(strInitVal);
	return initVal;
}

/*
 * initialize_peragg
 *
 * Almost same as in nodeAgg.c, except we don't support DISTINCT currently.
 */
static WindowStatePerAggData *
initialize_peragg(WindowAggState *winstate, WindowFunc *wfunc,
				  WindowStatePerAgg peraggstate)
{
	Oid			inputTypes[FUNC_MAX_ARGS];
	int			numArguments;
	HeapTuple	aggTuple;
	Form_pg_aggregate aggform;
	Oid			aggtranstype;
	AttrNumber	initvalAttNo;
	AclResult	aclresult;
	bool		use_ma_code;
	Oid			transfn_oid,
				invtransfn_oid,
				finalfn_oid;
	bool		finalextra;
	char		finalmodify;
	Expr	   *transfnexpr,
			   *invtransfnexpr,
			   *finalfnexpr;
	Datum		textInitVal;
	int			i;
	ListCell   *lc;

	numArguments = list_length(wfunc->args);

	i = 0;
	foreach(lc, wfunc->args)
	{
		inputTypes[i++] = exprType((Node *) lfirst(lc));
	}

	aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(wfunc->winfnoid));
	if (!HeapTupleIsValid(aggTuple))
		elog(ERROR, "cache lookup failed for aggregate %u",
			 wfunc->winfnoid);
	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

	/*
	 * Figure out whether we want to use the moving-aggregate implementation,
	 * and collect the right set of fields from the pg_attribute entry.
	 *
	 * It's possible that an aggregate would supply a safe moving-aggregate
	 * implementation and an unsafe normal one, in which case our hand is
	 * forced.  Otherwise, if the frame head can't move, we don't need
	 * moving-aggregate code.  Even if we'd like to use it, don't do so if the
	 * aggregate's arguments (and FILTER clause if any) contain any calls to
	 * volatile functions.  Otherwise, the difference between restarting and
	 * not restarting the aggregation would be user-visible.
	 */
	if (!OidIsValid(aggform->aggminvtransfn))
		use_ma_code = false;	/* sine qua non */
	else if (aggform->aggmfinalmodify == AGGMODIFY_READ_ONLY &&
			 aggform->aggfinalmodify != AGGMODIFY_READ_ONLY)
		use_ma_code = true;		/* decision forced by safety */
	else if (winstate->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING)
		use_ma_code = false;	/* non-moving frame head */
	else if (contain_volatile_functions((Node *) wfunc))
		use_ma_code = false;	/* avoid possible behavioral change */
	else
		use_ma_code = true;		/* yes, let's use it */
	if (use_ma_code)
	{
		peraggstate->transfn_oid = transfn_oid = aggform->aggmtransfn;
		peraggstate->invtransfn_oid = invtransfn_oid = aggform->aggminvtransfn;
		peraggstate->finalfn_oid = finalfn_oid = aggform->aggmfinalfn;
		finalextra = aggform->aggmfinalextra;
		finalmodify = aggform->aggmfinalmodify;
		aggtranstype = aggform->aggmtranstype;
		initvalAttNo = Anum_pg_aggregate_aggminitval;
	}
	else
	{
		peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
		peraggstate->invtransfn_oid = invtransfn_oid = InvalidOid;
		peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;
		finalextra = aggform->aggfinalextra;
		finalmodify = aggform->aggfinalmodify;
		aggtranstype = aggform->aggtranstype;
		initvalAttNo = Anum_pg_aggregate_agginitval;
	}

	/*
	 * ExecInitWindowAgg already checked permission to call aggregate function
	 * ... but we still need to check the component functions
	 */

	/* Check that aggregate owner has permission to call component fns */
	{
		HeapTuple	procTuple;
		Oid			aggOwner;

		procTuple = SearchSysCache1(PROCOID,
									ObjectIdGetDatum(wfunc->winfnoid));
		if (!HeapTupleIsValid(procTuple))
			elog(ERROR, "cache lookup failed for function %u",
				 wfunc->winfnoid);
		aggOwner = ((Form_pg_proc) GETSTRUCT(procTuple))->proowner;
		ReleaseSysCache(procTuple);

		aclresult = pg_proc_aclcheck(transfn_oid, aggOwner,
									 ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_FUNCTION,
						   get_func_name(transfn_oid));
		InvokeFunctionExecuteHook(transfn_oid);

		if (OidIsValid(invtransfn_oid))
		{
			aclresult = pg_proc_aclcheck(invtransfn_oid, aggOwner,
										 ACL_EXECUTE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_FUNCTION,
							   get_func_name(invtransfn_oid));
			InvokeFunctionExecuteHook(invtransfn_oid);
		}

		if (OidIsValid(finalfn_oid))
		{
			aclresult = pg_proc_aclcheck(finalfn_oid, aggOwner,
										 ACL_EXECUTE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_FUNCTION,
							   get_func_name(finalfn_oid));
			InvokeFunctionExecuteHook(finalfn_oid);
		}
	}

	/*
	 * If the selected finalfn isn't read-only, we can't run this aggregate as
	 * a window function.  This is a user-facing error, so we take a bit more
	 * care with the error message than elsewhere in this function.
	 */
	if (finalmodify != AGGMODIFY_READ_ONLY)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("aggregate function %s does not support use as a window function",
						format_procedure(wfunc->winfnoid))));

	/* Detect how many arguments to pass to the finalfn */
	if (finalextra)
		peraggstate->numFinalArgs = numArguments + 1;
	else
		peraggstate->numFinalArgs = 1;

	/* resolve actual type of transition state, if polymorphic */
	aggtranstype = resolve_aggregate_transtype(wfunc->winfnoid,
											   aggtranstype,
											   inputTypes,
											   numArguments);

	/* build expression trees using actual argument & result types */
	build_aggregate_transfn_expr(inputTypes,
								 numArguments,
								 0, /* no ordered-set window functions yet */
								 false, /* no variadic window functions yet */
								 aggtranstype,
								 wfunc->inputcollid,
								 transfn_oid,
								 invtransfn_oid,
								 &transfnexpr,
								 &invtransfnexpr);

	/* set up infrastructure for calling the transfn(s) and finalfn */
	fmgr_info(transfn_oid, &peraggstate->transfn);
	fmgr_info_set_expr((Node *) transfnexpr, &peraggstate->transfn);

	if (OidIsValid(invtransfn_oid))
	{
		fmgr_info(invtransfn_oid, &peraggstate->invtransfn);
		fmgr_info_set_expr((Node *) invtransfnexpr, &peraggstate->invtransfn);
	}

	if (OidIsValid(finalfn_oid))
	{
		build_aggregate_finalfn_expr(inputTypes,
									 peraggstate->numFinalArgs,
									 aggtranstype,
									 wfunc->wintype,
									 wfunc->inputcollid,
									 finalfn_oid,
									 &finalfnexpr);
		fmgr_info(finalfn_oid, &peraggstate->finalfn);
		fmgr_info_set_expr((Node *) finalfnexpr, &peraggstate->finalfn);
	}

	/* get info about relevant datatypes */
	get_typlenbyval(wfunc->wintype,
					&peraggstate->resulttypeLen,
					&peraggstate->resulttypeByVal);
	get_typlenbyval(aggtranstype,
					&peraggstate->transtypeLen,
					&peraggstate->transtypeByVal);

	/*
	 * initval is potentially null, so don't try to access it as a struct
	 * field. Must do it the hard way with SysCacheGetAttr.
	 */
	textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple, initvalAttNo,
								  &peraggstate->initValueIsNull);

	if (peraggstate->initValueIsNull)
		peraggstate->initValue = (Datum) 0;
	else
		peraggstate->initValue = GetAggInitVal(textInitVal,
											   aggtranstype);

	/*
	 * Initialize stuff needed to sort and deduplicate input to a
	 * DISTINCT-qualified aggregate.
	 */
	if (wfunc->windistinct)
	{
		/* the parser should have disallowed this case */
		if (list_length(wfunc->args) != 1)
			elog(ERROR, "DISTINCT is supported only for single-argument aggregates");

		peraggstate->isDistinct = true;

		peraggstate->distinctType = exprType(linitial(wfunc->args));
		peraggstate->distinctTypeByVal = get_typbyval(peraggstate->distinctType);
		peraggstate->distinctColl = exprCollation(linitial(wfunc->args));

		/* initialize support for sorting the argument */
		get_sort_group_operators(peraggstate->distinctType,
								 true, false, false,
								 &peraggstate->distinctLtOper,
								 NULL,
								 NULL,
								 NULL);
		memset(&peraggstate->distinctComparator, 0, sizeof(SortSupportData));

		peraggstate->distinctComparator.ssup_cxt = CurrentMemoryContext;
		peraggstate->distinctComparator.ssup_collation = peraggstate->distinctColl;
		peraggstate->distinctComparator.ssup_nulls_first = false;

		PrepareSortSupportFromOrderingOp(peraggstate->distinctLtOper,
										 &peraggstate->distinctComparator);
	}

	/*
	 * If the transfn is strict and the initval is NULL, make sure input type
	 * and transtype are the same (or at least binary-compatible), so that
	 * it's OK to use the first input value as the initial transValue.  This
	 * should have been checked at agg definition time, but we must check
	 * again in case the transfn's strictness property has been changed.
	 */
	if (peraggstate->transfn.fn_strict && peraggstate->initValueIsNull)
	{
		if (numArguments < 1 ||
			!IsBinaryCoercible(inputTypes[0], aggtranstype))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("aggregate %u needs to have compatible input type and transition type",
							wfunc->winfnoid)));
	}

	/*
	 * Insist that forward and inverse transition functions have the same
	 * strictness setting.  Allowing them to differ would require handling
	 * more special cases in advance_windowaggregate and
	 * advance_windowaggregate_base, for no discernible benefit.  This should
	 * have been checked at agg definition time, but we must check again in
	 * case either function's strictness property has been changed.
	 */
	if (OidIsValid(invtransfn_oid) &&
		peraggstate->transfn.fn_strict != peraggstate->invtransfn.fn_strict)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("strictness of aggregate's forward and inverse transition functions must match")));

	/*
	 * Moving aggregates use their own aggcontext.
	 *
	 * This is necessary because they might restart at different times, so we
	 * might never be able to reset the shared context otherwise.  We can't
	 * make it the aggregates' responsibility to clean up after themselves,
	 * because strict aggregates must be restarted whenever we remove their
	 * last non-NULL input, which the aggregate won't be aware is happening.
	 * Also, just pfree()ing the transValue upon restarting wouldn't help,
	 * since we'd miss any indirectly referenced data.  We could, in theory,
	 * make the memory allocation rules for moving aggregates different than
	 * they have historically been for plain aggregates, but that seems grotty
	 * and likely to lead to memory leaks.
	 */
	if (OidIsValid(invtransfn_oid))
		peraggstate->aggcontext =
			AllocSetContextCreate(CurrentMemoryContext,
								  "WindowAgg Per Aggregate",
								  ALLOCSET_DEFAULT_SIZES);
	else
		peraggstate->aggcontext = winstate->aggcontext;

	ReleaseSysCache(aggTuple);

	return peraggstate;
}

WindowAggState *
ExecInitVecWindowAgg(WindowAgg *node, EState *estate, int eflags)
{
	VecWindowAggState *vec_winstate;
	WindowAggState *winstate;
	Plan	   *outerPlan;
	ExprContext *econtext;
	ExprContext *tmpcontext;
	WindowStatePerFunc perfunc;
	WindowStatePerAgg peragg;
	int			frameOptions = node->frameOptions;
	int			numfuncs,
				wfuncno,
				numaggs,
				aggno;
	TupleDesc	scanDesc;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	vec_winstate = (VecWindowAggState *)palloc0(sizeof(VecWindowAggState));
	winstate = (WindowAggState *)vec_winstate;
	NodeSetTag(winstate, T_WindowAggState);

	winstate->ss.ps.plan = (Plan *) node;
	winstate->ss.ps.state = estate;
	winstate->ss.ps.ExecProcNode = ExecVecWindowAgg;

	/*
	 * Create expression contexts.  We need two, one for per-input-tuple
	 * processing and one for per-output-tuple processing.  We cheat a little
	 * by using ExecAssignExprContext() to build both.
	 */
	ExecAssignExprContext(estate, &winstate->ss.ps);
	tmpcontext = winstate->ss.ps.ps_ExprContext;
	winstate->tmpcontext = tmpcontext;
	ExecAssignExprContext(estate, &winstate->ss.ps);

	/* Create long-lived context for storage of partition-local memory etc */
	winstate->partcontext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "WindowAgg Partition",
							  ALLOCSET_DEFAULT_SIZES);

	/*
	 * Create mid-lived context for aggregate trans values etc.
	 *
	 * Note that moving aggregates each use their own private context, not
	 * this one.
	 */
	winstate->aggcontext =
		AllocSetContextCreate(CurrentMemoryContext,
                              "WindowAgg Aggregates",
                              ALLOCSET_DEFAULT_SIZES);

	/*
	 * WindowAgg nodes never have quals, since they can only occur at the
	 * logical top level of a query (ie, after any WHERE or HAVING filters)
	 */
	Assert(node->plan.qual == NIL);
	winstate->ss.ps.qual = NULL;

	/*
	 * initialize child nodes
	 */
	outerPlan = outerPlan(node);
	outerPlanState(winstate) = VecExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize source tuple type (which is also the tuple type that we'll
	 * store in the tuplestore and use in all our working slots).
	 */
	ExecCreateScanSlotFromOuterPlan(estate, &winstate->ss, &TTSOpsMinimalTuple);
	scanDesc = winstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	/* the outer tuple isn't the child's tuple, but always a minimal tuple */
	winstate->ss.ps.outeropsset = true;
	winstate->ss.ps.outerops = &TTSOpsMinimalTuple;
	winstate->ss.ps.outeropsfixed = true;

	/*
	 * tuple table initialization
	 */
	winstate->first_part_slot = ExecInitExtraTupleSlot(estate, scanDesc,
													   &TTSOpsMinimalTuple);
	winstate->agg_row_slot = ExecInitExtraTupleSlot(estate, scanDesc,
													&TTSOpsMinimalTuple);
	winstate->temp_slot_1 = ExecInitExtraTupleSlot(estate, scanDesc,
												   &TTSOpsMinimalTuple);
	winstate->temp_slot_2 = ExecInitExtraTupleSlot(estate, scanDesc,
												   &TTSOpsMinimalTuple);

	/*
	 * create frame head and tail slots only if needed (must create slots in
	 * exactly the same cases that update_frameheadpos and update_frametailpos
	 * need them)
	 */
	winstate->framehead_slot = winstate->frametail_slot = NULL;

	if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS))
	{
		if (((frameOptions & FRAMEOPTION_START_CURRENT_ROW) &&
			 node->ordNumCols != 0) ||
			(frameOptions & FRAMEOPTION_START_OFFSET))
			winstate->framehead_slot = ExecInitExtraTupleSlot(estate, scanDesc,
															  &TTSOpsMinimalTuple);
		if (((frameOptions & FRAMEOPTION_END_CURRENT_ROW) &&
			 node->ordNumCols != 0) ||
			(frameOptions & FRAMEOPTION_END_OFFSET))
			winstate->frametail_slot = ExecInitExtraTupleSlot(estate, scanDesc,
															  &TTSOpsMinimalTuple);
	}

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&winstate->ss.ps, &TTSOpsVecTuple);
	ExecAssignProjectionInfo(&winstate->ss.ps, NULL);

	/* Set up data for comparing tuples */
	if (node->partNumCols > 0)
		winstate->partEqfunction =
			execTuplesMatchPrepare(scanDesc,
								   node->partNumCols,
								   node->partColIdx,
								   node->partOperators,
								   node->partCollations,
								   &winstate->ss.ps);

	if (node->ordNumCols > 0)
		winstate->ordEqfunction =
			execTuplesMatchPrepare(scanDesc,
								   node->ordNumCols,
								   node->ordColIdx,
								   node->ordOperators,
								   node->ordCollations,
								   &winstate->ss.ps);

	/*
	 * WindowAgg nodes use aggvalues and aggnulls as well as Agg nodes.
	 */
	numfuncs = winstate->numfuncs;
	numaggs = winstate->numaggs;
	econtext = winstate->ss.ps.ps_ExprContext;
	econtext->ecxt_aggvalues = (Datum *) palloc0(sizeof(Datum) * numfuncs);
	econtext->ecxt_aggnulls = (bool *) palloc0(sizeof(bool) * numfuncs);

	/*
	 * allocate per-wfunc/per-agg state information.
	 */
	perfunc = (WindowStatePerFunc) palloc0(sizeof(WindowStatePerFuncData) * numfuncs);
	peragg = (WindowStatePerAgg) palloc0(sizeof(WindowStatePerAggData) * numaggs);
	winstate->perfunc = perfunc;
	winstate->peragg = peragg;

	wfuncno = -1;
	aggno = -1;
	foreach(l, winstate->funcs)
	{
		WindowFuncExprState *wfuncstate = (WindowFuncExprState *) lfirst(l);
		WindowFunc *wfunc = wfuncstate->wfunc;
		WindowStatePerFunc perfuncstate;
		AclResult	aclresult;
		int			i;

		if (wfunc->winref != node->winref)		/* planner screwed up? */
			elog(ERROR, "WindowFunc with winref %u assigned to WindowAgg with winref %u",
				 wfunc->winref, node->winref);

		/* Look for a previous duplicate window function */
		for (i = 0; i <= wfuncno; i++)
		{
			if (equal(wfunc, perfunc[i].wfunc) &&
				!contain_volatile_functions((Node *) wfunc))
				break;
		}
		if (i <= wfuncno)
		{
			/* Found a match to an existing entry, so just mark it */
			wfuncstate->wfuncno = i;
			continue;
		}

		/* Nope, so assign a new PerAgg record */
		perfuncstate = &perfunc[++wfuncno];

		/* Mark WindowFunc state node with assigned index in the result array */
		wfuncstate->wfuncno = wfuncno;

		/* Check permission to call window function */
		aclresult = pg_proc_aclcheck(wfunc->winfnoid, GetUserId(),
									 ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_FUNCTION,
						   get_func_name(wfunc->winfnoid));
		InvokeFunctionExecuteHook(wfunc->winfnoid);

		/* Fill in the perfuncstate data */
		perfuncstate->wfuncstate = wfuncstate;
		perfuncstate->wfunc = wfunc;
		perfuncstate->numArguments = list_length(wfuncstate->args);
		perfuncstate->winCollation = wfunc->inputcollid;

		get_typlenbyval(wfunc->wintype,
						&perfuncstate->resulttypeLen,
						&perfuncstate->resulttypeByVal);

		/*
		 * If it's really just a plain aggregate function, we'll emulate the
		 * Agg environment for it.
		 */
		perfuncstate->plain_agg = wfunc->winagg;
		if (wfunc->winagg)
		{
			WindowStatePerAgg peraggstate;

			perfuncstate->aggno = ++aggno;
			peraggstate = &winstate->peragg[aggno];
			initialize_peragg(winstate, wfunc, peraggstate);
			peraggstate->wfuncno = wfuncno;
		}
		else
		{
			WindowObject winobj = makeNode(WindowObjectData);

			winobj->winstate = (WindowAggState *)winstate;
			winobj->argstates = wfuncstate->args;
			winobj->localmem = NULL;
			perfuncstate->winobj = winobj;

			/* It's a real window function, so set up to call it. */
			fmgr_info_cxt(wfunc->winfnoid, &perfuncstate->flinfo,
						  econtext->ecxt_per_query_memory);
			fmgr_info_set_expr((Node *) wfunc, &perfuncstate->flinfo);
		}
	}

	/* Update numfuncs, numaggs to match number of unique functions found */
	winstate->numfuncs = wfuncno + 1;
	winstate->numaggs = aggno + 1;

	/* Set up WindowObject for aggregates, if needed */
	if (winstate->numaggs > 0)
	{
		WindowObject agg_winobj = makeNode(WindowObjectData);

		agg_winobj->winstate = (WindowAggState *)winstate;
		agg_winobj->argstates = NIL;
		agg_winobj->localmem = NULL;
		/* make sure markptr = -1 to invalidate. It may not get used */
		agg_winobj->markptr = -1;
		agg_winobj->readptr = -1;
		winstate->agg_winobj = agg_winobj;
	}

	/* copy frame options to state node for easy access */
	winstate->frameOptions = node->frameOptions;

	/* initialize frame bound offset expressions */
	winstate->startOffset = ExecInitExpr((Expr *) node->startOffset,
										 (PlanState *) winstate);
	winstate->endOffset = ExecInitExpr((Expr *) node->endOffset,
									   (PlanState *) winstate);

	/* Lookup in_range support functions if needed */
	if (OidIsValid(node->startInRangeFunc))
		fmgr_info(node->startInRangeFunc, &winstate->startInRangeFunc);
	if (OidIsValid(node->endInRangeFunc))
		fmgr_info(node->endInRangeFunc, &winstate->endInRangeFunc);
	winstate->inRangeColl = node->inRangeColl;
	winstate->inRangeAsc = node->inRangeAsc;
	winstate->inRangeNullsFirst = node->inRangeNullsFirst;

	winstate->start_offset_var_free =
		!contain_var_clause(node->startOffset) &&
		!contain_volatile_functions(node->startOffset);
	winstate->end_offset_var_free =
		!contain_var_clause(node->endOffset) &&
		!contain_volatile_functions(node->endOffset);

	winstate->all_first = true;
	winstate->partition_spooled = false;
	winstate->more_partitions = false;

	/* Init Arrow execute plan */
	PostBuildVecPlan((PlanState *)vec_winstate, &vec_winstate->estate);

	return winstate;
}

static void
ExecEagerFreeVecWindowAgg(VecWindowAggState *vnode)
{

	if (vnode->plan)
		ARROW_FREE(GArrowExecutePlan, &vnode->plan);
	if (vnode->state)
		ARROW_FREE(GArrowAggregationNodeState, &vnode->state);

	if (vnode->source_schema)
		ARROW_FREE(GArrowSchema, &vnode->source_schema);
	if (vnode->origin_rb)
		ARROW_FREE(GArrowRecordBatch, &vnode->origin_rb);
	FreeVecExecuteState(&vnode->estate);
	if (vnode->result_slot)
		ExecClearTuple(vnode->result_slot);
}

void
ExecEndVecWindowAgg(WindowAggState *node)
{
	PlanState  	*outerPlan;
	VecWindowAggState *vnode;
	vnode = (VecWindowAggState *) node;
	/*
	 * We don't actually free any ExprContexts here (see comment in
	 * ExecFreeExprContext), just unlinking the output one from the plan node
	 * suffices.
	 */
	ExecFreeExprContext(&node->ss.ps);

	/* clean up tuple table */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	if (VECSLOT(node->ss.ps.ps_ResultTupleSlot)->vec_schema.schema)
		ARROW_FREE(GArrowSchema, &VECSLOT(node->ss.ps.ps_ResultTupleSlot)->vec_schema.schema);

	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecEagerFreeVecWindowAgg(vnode);
	outerPlan = outerPlanState(node);
	VecExecEndNode(outerPlan);
}

static TupleTableSlot*
ExecVecWindowAgg(PlanState *pstate)
{
	WindowAggState    *node  = castNode(WindowAggState, pstate);
	VecWindowAggState *vnode = (VecWindowAggState *) node;

	int rows, rest, length;
	g_autoptr(GArrowRecordBatch) slice_rb = NULL;

	/*
	 * FIXME: the major part the ExecVecWindowAgg() and ExecVecAgg() should be
	 * merged and moved to the excutor function like VecExecProcNodeFirst() or
	 * VecExecProcNodeGPDB();
	 */
	if (vnode->offset >= vnode->rows)
	{
		if (vnode->result_slot)
			ExecClearTuple(vnode->result_slot);
		if (vnode->origin_rb)
			ARROW_FREE_BATCH(&vnode->origin_rb);
		vnode->result_slot = NULL;
		vnode->offset      = 0;
		vnode->rows        = 0;

		TupleTableSlot *slot = ExecuteVecPlan(&vnode->estate);
		if (TupIsNull(slot))
		{
			return slot;
		}

		rows = GetNumRows(slot);
		if (rows <= max_batch_size)
		{
			return slot;
		}

		vnode->result_slot = slot;
		vnode->origin_rb   = g_object_ref(GetBatch(slot));
		vnode->offset      = 0;
		vnode->rows        = GetNumRows(slot);
	}

	ExecClearTuple(vnode->result_slot);

	rest   = vnode->rows - vnode->offset;
	length = rest > max_batch_size ? max_batch_size : rest;

	slice_rb = garrow_record_batch_slice(vnode->origin_rb, vnode->offset, length);

	vnode->offset += length;
	g_autoptr(GError) error = NULL;
	ExecStoreBatch(vnode->result_slot,  slice_rb);

	return vnode->result_slot;
}

void
ExecSquelchVecWindowAgg(WindowAggState *node)
{
	VecWindowAggState *vnode = (VecWindowAggState *) node;
	ExecEagerFreeVecWindowAgg(vnode);
	ExecVecSquelchNode(outerPlanState(node));
}
