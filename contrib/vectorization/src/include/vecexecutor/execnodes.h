/*-------------------------------------------------------------------------
 * execnodes.h
 *	  definitions for vectorized executor state nodes
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/include/vecexecutor/execnodes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VEC_EXEC_NODES_H
#define VEC_EXEC_NODES_H

#include "access/heapam.h"
#include "nodes/execnodes.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbhash.h"
#include "executor/nodeAgg.h"

#include "utils/arrow.h"
#include "utils/vecheap.h"
#include "nodes/execnodes.h"
#include "utils/tuptable_vec.h"

/* runtime Arrow plan state */
typedef struct VecExecuteState
{
	GArrowExecutePlan *plan;
	bool started; /* plan execution has been started */
	TupleTableSlot *slot; /* slot for plan result*/
	bool pipeline;
	GArrowRecordBatchReader *reader;
	List *resqueue;
} VecExecuteState;

typedef struct VecSeqScanState
{
	SeqScanState base;
	VecExecuteState vestate;
	int       	*columnmap;
	/* base.ss.ss_ScanTupleSlot is TTSOpsVirtual for abi,
	 * this slot is TTSOpsVecTuple for imported batch. */
	TupleTableSlot *vscanslot;

	Size		pscan_len;		/* size of parallel heap scan descriptor */
	struct AOCSScanDescData *ss_currentScanDesc_aocs;
	VecDesc   	vecdesc;
	ResDesc   	resdesc;
} VecSeqScanState;

typedef struct VecForeignScanState
{
	ForeignScanState base;
	VecExecuteState vestate;
	int       	*columnmap;
	/* base.ss.ss_ScanTupleSlot is TTSOpsVirtual for abi,
	 * this slot is TTSOpsVecTuple for imported batch. */
	TupleTableSlot *vscanslot;

	GArrowSchema *schema;
} VecForeignScanState;

typedef struct VecSequenceState
{
	SequenceState base;	
	VecExecuteState estate;
} VecSequenceState;

/* ----------------
 * VecResultState information
 * ----------------
 */
typedef struct VecResultState
{
	ResultState base;
	VecExecuteState estate;
	void *hash_projector;
} VecResultState;

typedef struct VecSubqueryScanState
{
	SubqueryScanState base;
	VecExecuteState estate;
} VecSubqueryScanState;

typedef struct VecMotionState
{
	MotionState base;	
	TupleDesc 	transTupDesc;
	struct vecheap *tupleheap;

	/* FIXME: we need support vec hash in the future. */
	/* vectorization */
	//struct CdbHashVec *cdbhashvec;	/* vec hash api object */
	bool return_tup;

	int *offset;
	char *memtups;

	TupleTableSlot *outer_vecslot;
	TupleTableSlot *rowslot;
	TupleTableSlot *vecslot;
	bool islast;

	bool is_vec_hash;

	void ***segments;  /* nth item in segments is a batch of of builders used to
						* make the slot sent to nth segment.
						*/

	/* FIXME: express will be support in the future */
	bool *hash_cols;   /* proj col for hash */
	void *hash_projector;
	List *hashExprsGandivaNodes;
	GArrowUInt32Array* random_const_array_template;
} VecMotionState;



typedef struct VecSortState
{
	SortState 	base;
	VecExecuteState estate;

	bool		skip; /* true if upper node is groupagg, sort done in groupagg */
	bool 		started;
} VecSortState;

typedef struct VecAppendState
{
	AppendState base;
	VecExecuteState estate;
	GArrowSchema *schema;
} VecAppendState;


typedef struct VecAggState
{
	AggState base;

	VecExecuteState estate;

	/* Arrow plan control params */
	GArrowExecutePlan			*plan;
	GArrowAggregationNodeState	*state; 
	List						*rbs; 	/* n batches of data */

	/* source schema */
	GArrowSchema *source_schema;

	/* outplan slot */
	TupleTableSlot *outplan_slot;

	/* for streaming loop */
	bool init_arrow;

	/* agg proj node expression */
	void *proj;

	/* these fields are used in AGG_HASHED and initial pass */
	uint32		*hashkeys;

	/* these fields are used in AGG_HASHED and agg_retrieve_hash_table_vec */
	void		**entries;
	TupleTableSlot *outer_vecslot;

	ExprContext *veccontext;

	/* used by hashagg with arrow native */
	GArrowSchema *hashagg_input_schema;
	GArrowRecordBatch *hashagg_input_rb;
	GArrowDataType *decimal256_dt;
	GArrowDataType *int64_dt;

	char **aggfn_arg;
	char **aggfn_rs;
	char **aggfn_arg_count;
	char **aggfn_arg_sum;
	char **aggfn_rs_count;
	char **aggfn_rs_sum;
	char **grpkeys;

	/* used by slice recordbatch */
	TupleTableSlot *result_slot;
	GArrowRecordBatch *origin_rb;
	int offset;
	int rows;


	bool	sorted;
	bool	streaming;
	int		streamgroups; /* the number of the groups per batch */
	int		curgroups;  /* the number of the groups in GroupByNode currently */


} VecAggState;

typedef struct VecNestLoopState
{
	NestLoopState base;	
	VecExecuteState estate;
} VecNestLoopState;

typedef struct VecMaterialState
{
	MaterialState base;
	bool is_skip; /* can skip material, for example it combo with nestloop */
}VecMaterialState;

typedef struct VecShareInputScanState
{
	ShareInputScanState base;
}VecShareInputScanState;

typedef struct VecAggStatePerAggData 
{
	AggStatePerAggData base;

	void *projector;

	TupleDesc	evaldesc;		/* descriptor of input tuples */
	/*
	 * Slots for holding the evaluated input arguments.  These are set up
	 * during ExecInitAgg() and then used for each input row.
	 */
	TupleTableSlot *evalslot;	/* current input tuple */

	/* these fields are used in aggfunc(distinct xxx) */
	Datum		*grp_keys_datums;
	bool		*grp_keys_isnull;
	GList		*order_slices;
} VecAggStatePerAggData;

typedef struct VecAggStatePerAggData *VecAggStatePerAgg;

typedef struct VecWindowAggState
{
	WindowAggState base;

	VecExecuteState estate;

	/* Arrow plan control params */
	GArrowExecutePlan			*plan;
	GArrowAggregationNodeState	*state; 
	List						*rbs; 	/* n batches of data */

	/* source schema */
	GArrowSchema *source_schema;

	/* outplan slot */
	TupleTableSlot *outplan_slot;

	/* for streaming loop */
	bool init_arrow;

	ExprContext *veccontext;

	/* used by slice recordbatch */
	TupleTableSlot *result_slot;
	GArrowRecordBatch *origin_rb;
	int offset;
	int rows;

} VecWindowAggState;

/*
 * This struct is the data actually passed to an fmgr-called function.
 */
typedef struct FunctionCallInfoData
{
	FmgrInfo   *flinfo;			/* ptr to lookup info used for this call */
	fmNodePtr	context;		/* pass info about context of call */
	fmNodePtr	resultinfo;		/* pass or return extra info about result */
	Oid			fncollation;	/* collation for function to use */
	bool		isnull;			/* function must set true if result is NULL */
	short		nargs;			/* # arguments actually passed */
	Datum		arg[FUNC_MAX_ARGS];		/* Arguments passed to function */
	bool		argnull[FUNC_MAX_ARGS]; /* T if arg[i] is actually NULL */
} FunctionCallInfoData;

/* ----------------
 *  FuncExprState node
 *  
 *  Although named for FuncExpr, this is also used for OpExpr, DistinctExpr,
 *  and NullIf nodes; be careful to check what xprstate.expr is actually
 *  pointing at!
 *  ----------------
 */
typedef struct FuncExprState
{
	ExprState	xprstate;
	List	   *args;			/* states of argument expressions */

	/*
  	 * Function manager's lookup info for the target function.  If func.fn_oid
  	 * is InvalidOid, we haven't initialized it yet (nor any of the following
  	 * fields).
  	 */
	FmgrInfo	func;

	/*
  	 * For a set-returning function (SRF) that returns a tuplestore, we keep
  	 * the tuplestore here and dole out the result rows one at a time. The
  	 * slot holds the row currently being returned.
  	 */
	Tuplestorestate *funcResultStore;
	TupleTableSlot *funcResultSlot;

	/*
         * In some cases we need to compute a tuple descriptor for the function's
         * output.  If so, it's stored here.
         */
	TupleDesc	funcResultDesc;
	bool		funcReturnsTuple;		/* valid when funcResultDesc isn't
										 * NULL */

	/*
  	 * setArgsValid is true when we are evaluating a set-returning function
  	 * that uses value-per-call mode and we are in the middle of a call
  	 * series; we want to pass the same argument values to the function again
  	 * (and again, until it returns ExprEndResult).  This indicates that
  	 * fcinfo_data already contains valid argument data.
  	 */
	bool		setArgsValid;

	/*
  	 * Flag to remember whether we found a set-valued argument to the
  	 * function. This causes the function result to be a set as well. Valid
  	 * only when setArgsValid is true or funcResultStore isn't NULL.
  	 */
	bool		setHasSetArg;	/* some argument returns a set */

	/*
  	 * Flag to remember whether we have registered a shutdown callback for
  	 * this FuncExprState.  We do so only if funcResultStore or setArgsValid
  	 * has been set at least once (since all the callback is for is to release
  	 * the tuplestore or clear setArgsValid).
  	 */
	bool		shutdown_reg;	/* a shutdown callback is registered */

	/*
  	 * Call parameter structure for the function.  This has been initialized
  	 * (by InitFunctionCallInfoData) if func.fn_oid is valid.  It also saves
  	 * argument values between calls, when setArgsValid is true.
  	 */
	FunctionCallInfoData fcinfo_data;

	/* Fast Path */
	ExprState  *fp_arg[2];
	Datum		fp_datum[2];
	bool		fp_null[2];
} FuncExprState;

/* ----------------
 *
 * ScalarArrayOpExprState node
 * 
 * This is a FuncExprState plus some additional data.
 *  ----------------
 */
typedef struct ScalarArrayOpExprState
{
	FuncExprState fxprstate;
	/* Cached info about array element type */
	Oid			element_type;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	/* Fast path x in ('A', 'B', 'C') */
	int			fp_n;
	int		   *fp_len;
	Datum	   *fp_datum;
} ScalarArrayOpExprState;

/* ----------------
 *  GenericExprState node
 *  
 *  This is used for Expr node types that need no local run-time state,
 *  but have one child Expr node.
 *  ----------------
 */
typedef struct GenericExprState
{
	ExprState	xprstate;
	ExprState  *arg;			/* state of my child node */
} GenericExprState;

typedef struct VecAggrefExprState
{
	ExprState	xprstate;
	List	   *aggdirectargs;	/* states of direct-argument expressions */
	List	   *args;			/* states of aggregated-argument expressions */
	ExprState  *aggfilter;		/* state of FILTER expression, if any */
	int			aggno;			/* ID number for agg within its plan node */
	char        name[30];
} VecAggrefExprState;
typedef struct FuncExprState VecFuncExprState;

typedef struct CdbHashVec
{
        void                *hash;                      /* The result hash value                                                        */
        int          hash_size;
        int                     numsegs;                    /* number of segments in Greenplum Database used for
                                                                 * partitioning  */
        CdbHashReduce reducealg;            /* the algorithm used for reducing to buckets               */
        bool            is_legacy_hash;

        //uint32          numLogicParts;  /* consistent hash-ring index*/
        int                *segmapping;         /* mapping between logic part and physical part */
        void       **segmapping_scalar; /* scalar array for mapping between logic part and physical part */
        bool            useGPDBHash;

        int                     natts;
        FmgrInfo   *hashfuncs;

} CdbHashVec;

typedef struct VecHashJoinState
{
	HashJoinState base;
	VecExecuteState estate;
	/* fields for vector engine */
	ExprContext    *hashkeys_econtext;

	/* used by slice recordbatch */
	TupleTableSlot *result_slot;
	GArrowRecordBatch *origin_rb;
	int offset;
	int nrows;
	bool is_left; 

	/* for semi/anti join with joinqual */
	bool joinqual_pushdown; 
	GList *semi_anti_filter;
	int left_attr_in_joinqual;
	int right_attr_in_joinqual;
} VecHashJoinState;

typedef struct VecHashState
{
	HashState base;
} VecHashState;

typedef struct VecAssertOpState
{
	AssertOpState base;
	VecExecuteState estate;
} VecAssertOpState;

typedef struct VecLimitState
{
	LimitState base;
	VecExecuteState estate;
} VecLimitState;

#endif							/* VEC_EXEC_NODES_H */
