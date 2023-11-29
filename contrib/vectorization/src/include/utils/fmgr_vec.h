/*--------------------------------------------------------------------
 * fmgr_vec.h
 *	  Definitions for the Postgres function manager and function-call
 *	  interface, the vectorized version
 *
 * This file must be included by all Postgres modules that either define
 * or call fmgr-callable functions.

 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/include/utils/fmgr_vec.h
 *
 *--------------------------------------------------------------------
 */
#ifndef FMGR_VEC_H
#define FMGR_VEC_H

#include "postgres.h"

#include "fmgr.h"
#include "utils/arrow.h"

#define PG_VEC_FUNCTION_ARGS	FunctionCallInfoVec fcinfo
#define PG_VEC_RETURN_POINTER(x) return PointerGetDatum(g_steal_pointer(&x))
#define PG_VEC_GETARG(n) garrow_copy_ptr(PG_GETARG_POINTER(n))
#define PG_VEC_GETARG_DATUM(n) GARROW_DATUM(garrow_copy_ptr(PG_GETARG_POINTER(n)))
#define PG_NO_TRANS	fcinfo->notransvalue
#define PG_HAS_BATCH (fcinfo->argbatch != NULL)
#define PG_GETARG_BATCH (fcinfo->argbatch)
typedef GArrowFunctionOptions *(*ArrowFunctionOpts)(int numargs);

typedef struct FunctionCallInfoBaseDataVec *FunctionCallInfoVec;
typedef Datum (*PGVecFunction) (FunctionCallInfoVec fcinfo);
/*
 * This struct is the data actually passed to an fmgr-called function.
 */
typedef struct FunctionCallInfoBaseDataVec
{
	/* For VecAgg functions */
	bool        notransvalue;   /* VecAgg functions handle noTransvalue in itself*/
	void	   *argbatch;		/* record batch as argument */

	FmgrInfo   *flinfo;			/* ptr to lookup info used for this call */
	fmNodePtr	context;		/* pass info about context of call */
	fmNodePtr	resultinfo;		/* pass or return extra info about result */
	Oid			fncollation;	/* collation for function to use */
	bool		isnull;			/* function must set true if result is NULL */
	short		nargs;			/* # arguments actually passed */
	NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
} FunctionCallInfoBaseDataVec;

/*
 * This table stores info about all the built-in functions (ie, functions
 * that are compiled into the Postgres executable).  The table entries are
 * required to appear in Oid order, so that binary search can be used.
 */
typedef struct ArrowFmgr
{
	const char *funcName; /* PG name of aggfnoid, got from pg_aggregate table*/
	Oid			fnoid;
	const char *transfn; /* Arrow aggregation transfn*/
	const char *finalfn; /* Arrow aggregation finalfn*/
	const char *simplefn; /* Arrow aggregation for AGGSPLIT_SIMPLE */
} ArrowFmgr;

typedef struct FuncTable
{
	/* default function name */
	const char *funcName;
	/* hash function name */
	const char *hashFuncName;
	/* ditinct function name */
	const char *distFuncName;
	/* hash ditinct function name */
	const char *hashDistFuncName;
	/* function */
	ArrowFunctionOpts getOption;
} FuncTable;

extern const ArrowFmgr arrow_fmgr_builtins[];
typedef struct
{
	Oid                     foid;                   /* OID of the function */
	const char *funcName;           /* C name of the function */
	short		nargs;			/* 0..FUNC_MAX_ARGS, or -1 if variable count */
	bool		strict;			/* T if function is "strict" */
	bool		retset;			/* T if function returns a set */
	const char *opname;         /* gandiva operation name */
} FmgrVecBuiltin;
#define arrow_fmgr_length sizeof(arrow_fmgr_builtins) / sizeof(ArrowFmgr)

extern const FuncTable arrow_func_tables[];
#define arrow_functables_length sizeof(arrow_func_tables) / sizeof(FuncTable)

extern const ArrowFmgr *get_arrow_fmgr(Oid foid);
extern const FuncTable *get_arrow_functable(const char *name);
/*
 * Get vector function information.
 */
extern const FmgrVecBuiltin *fmgr_isbuiltin_vec(Oid id);

extern void fmgr_info_vec(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt);

extern Datum FunctionCall2Args(const char *fname, void *arg1, void *arg2);
extern Datum FunctionCall1Args(const char *fname, void *arg1);
extern Datum FunctionCall2Args(const char *fname, void *arg1, void *arg2);
extern Datum FunctionCall3Args(const char *fname, void *arg1, void *arg2, void* arg3);
extern Datum DirectCallVecFunc2Args(const char *fname, void *arg1, void *arg2);
extern Datum DirectCallVecFunc1Args(const char *fname, void *arg1);
extern Datum DirectCallVecFunc2ArgsAndU32ArrayRes(const char *fname, void *arg1, void *arg2);
extern Datum DirectCallVecFunc3ArgsAndU32ArrayRes(const char *fname, void *arg1, void *arg2, void* arg3);


/* free the intermediate arrays */
static inline void free_fmgr_vec(FunctionCallInfo fcinfo)
{
	int i;
	for (i = 0; i < fcinfo->nargs; i++)
	{
		if (fcinfo->args[i].value)
		{
			ARROW_FREE(GArrowDatum, &fcinfo->args[i].value);
		}
	}
}

#endif /* FMGR_VEC_H */
