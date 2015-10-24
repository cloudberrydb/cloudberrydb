#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pg_type.h"      /* oids of known types */
#include "utils/builtins.h"       /* Builtin functions, including lower() */
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>

/* Do the module magic dance */
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
PG_FUNCTION_INFO_V1(wordsplit);
PG_FUNCTION_INFO_V1(int4_accum);
PG_FUNCTION_INFO_V1(int8_add);
PG_FUNCTION_INFO_V1(tran);
PG_FUNCTION_INFO_V1(final);
PG_FUNCTION_INFO_V1(retcomposite);


/* Declare the functions as module exports */

int tran(PG_FUNCTION_ARGS);
int final(PG_FUNCTION_ARGS);
Datum retcomposite(PG_FUNCTION_ARGS);
Datum wordsplit(PG_FUNCTION_ARGS);
Datum int4_accum(PG_FUNCTION_ARGS);
Datum int8_add(PG_FUNCTION_ARGS);

const char* wordsplit_str = "gpmrdemo:wordsplit()";
const char* accum_str = "gpmrdemo:int4_accum()";
const char* add_str   = "gpmrdemo:int8_add()";


/* 
 * In this case the state structure is so simple that this is unnecessary,
 * but frequently a more complex user state is needed, this demonstrates
 * how that may be accomplished.
 */
typedef struct {
	char *string;
} wordsplit_state;

/* Define the functions */
Datum 
wordsplit(PG_FUNCTION_ARGS)
{
	FuncCallContext     *funcctx;
    TupleDesc            tupdesc;
	wordsplit_state     *myState;
	char                *word;
	HeapTuple            res;
	Datum                result;
	Datum                values[2];
	bool                 nulls[2] = {false, false};

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
        MemoryContext   oldcontext;
		
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        /* Bless the tuple descriptor for later use */
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Allocate a cross call user state */
		funcctx->user_fctx = palloc0(sizeof(wordsplit_state));
		myState = (wordsplit_state*) funcctx->user_fctx;

		/* Extract needed information from input parameters */
		if (PG_ARGISNULL(0))
		{
			myState->string = NULL;
		}
		else
		{
			/* Fetch the input parameter */
			Datum d = PG_GETARG_DATUM(0);

			/* Use a builtin function definition to convert it to lower case */
			d = DirectFunctionCall1(lower, d);

			/* Convert the datum into a cstring for our use */
			myState->string = TextDatumGetCString(d);
		}
		
		/* Restore the per-call memory context */
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
	myState = (wordsplit_state*) funcctx->user_fctx;


	/* Handle the case of null input */
	if (!myState->string)
	{
		SRF_RETURN_DONE(funcctx);
	}

	/*
	 * Begin messy string handling in "C", this would be so much nicer
	 * in any more advanced language...
	 */

	/* Scan forward to the next word character */
	while (myState->string[0] < 'a' || myState->string[0] > 'z')
	{
		/* If we hit the end then we are done */
		if (myState->string[0] == '\0')
			SRF_RETURN_DONE(funcctx);
		myState->string++;
	}

	/* Find the next word */
	word = myState->string;

	/* Scan forward until the end of the word */
	while (myState->string[0] >= 'a' && myState->string[0] <= 'z')
		myState->string++;

	/* 
	 * If we terminated on whitespace then advance to the next non-whitespace
	 * character.
	 */
	if (myState->string[0] != '\0')
	{
		myState->string[0] = '\0';
		myState->string++;
	}

	/*
	 * We have our word, and now need to construct the return tuple of:
	 *    (word, 1)
	 */
	values[0] = CStringGetTextDatum(word);
	values[1] = Int32GetDatum(1);

	/* Construct the Tuple */
	res = heap_form_tuple(funcctx->tuple_desc, values, nulls);

	/* Convert the Tuple into a Datum */
	result = HeapTupleGetDatum(res);

	/* Return the tuple */
	SRF_RETURN_NEXT(funcctx, result);
}

Datum 
int4_accum(PG_FUNCTION_ARGS)
{
	int64   state;
    int32   value;

	/*
	 * GUARD against an incorrectly defined SQL function by verifying
	 * that the parameters are the types we are expecting: 
	 *    int4_accum(int64, int32) => int64
	 */
	if (PG_NARGS() != 2)
	{
		elog(ERROR, "%s defined with %d arguments, expected 2", 
			 accum_str, PG_NARGS() );
	}
	if (get_fn_expr_argtype(fcinfo->flinfo, 0) != INT8OID ||
		get_fn_expr_argtype(fcinfo->flinfo, 1) != INT4OID)
	{
		elog(ERROR, "%s defined with invalid types, expected (int8, int4)",
			 accum_str );
	}
	if (get_fn_expr_rettype(fcinfo->flinfo) != INT8OID)
	{
		elog(ERROR, "%s defined with invalid return type, expected int8",
			 accum_str );
	}

	/* 
	 * GUARD against NULL input:
	 *  - IF both are null return NULL
	 *  - otherwise treat NULL as a zero value
	 */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();
	state = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT64(0);
	value = PG_ARGISNULL(1) ? 0 : PG_GETARG_INT32(1);

	/* Do the math and return the result */
    PG_RETURN_INT64(state + value);
}



Datum 
int8_add(PG_FUNCTION_ARGS)
{
	int64   state1;
    int64   state2;

	/*
	 * GUARD against an incorrectly defined SQL function by verifying
	 * that the parameters are the types we are expecting: 
	 *    int8_add(int64, int64) => int64
	 */
	if (PG_NARGS() != 2)
	{
		elog(ERROR, "%s defined with %d arguments, expected 2", 
			 add_str, PG_NARGS() );
	}
	if (get_fn_expr_argtype(fcinfo->flinfo, 0) != INT8OID ||
		get_fn_expr_argtype(fcinfo->flinfo, 1) != INT8OID)
	{
		elog(ERROR, "%s defined with invalid types, expected (int8, int8)",
			 add_str );
	}
	if (get_fn_expr_rettype(fcinfo->flinfo) != INT8OID)
	{
		elog(ERROR, "%s defined with invalid return type, expected int8",
			 add_str );
	}

	/* 
	 * GUARD against NULL input:
	 *  - IF both are null return NULL
	 *  - otherwise treat NULL as a zero value
	 */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();
	state1 = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT64(0);
	state2 = PG_ARGISNULL(1) ? 0 : PG_GETARG_INT64(1);

	/* Do the math and return the result */
    PG_RETURN_INT64(state1 + state2);
}

int tran(PG_FUNCTION_ARGS)
{
    int state = PG_GETARG_INT32(0);
    int arg2 = PG_GETARG_INT32(1);

    if (state > 0)
    {
        arg2 = state + arg2;
    }
    return arg2;
}

int final(PG_FUNCTION_ARGS)
{
    int a = PG_GETARG_INT32(0);
    
    PG_RETURN_INT32(a);
}

Datum retcomposite(PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    int                  call_cntr;
    int                  max_calls;
    TupleDesc            tupdesc;
    AttInMetadata       *attinmeta;

     /* stuff done only on the first call of the function */
     if (SRF_IS_FIRSTCALL())
     {
        MemoryContext   oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* total number of tuples to be returned */
        //funcctx->max_calls = PG_GETARG_UINT32(0);
        funcctx->max_calls = 1;

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        /*
         * generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = 1;
    attinmeta = funcctx->attinmeta;
 
    if (call_cntr < max_calls)    /* do when there is more left to send */
    {
        char       **values;
        HeapTuple    tuple;
        Datum        result;

        /*
         * Prepare a values array for building the returned tuple.
         * This should be an array of C strings which will
         * be processed later by the type input functions.
         */
        values = (char **) palloc(3 * sizeof(char *));
        values[0] = (char *) palloc(16 * sizeof(char));
        values[1] = (char *) palloc(16 * sizeof(char));
        values[2] = (char *) palloc(16 * sizeof(char));

        snprintf(values[0], 16, "%d", 1*  PG_GETARG_INT32(0));
        snprintf(values[1], 16, "%d", 2*  PG_GETARG_INT32(0));
        snprintf(values[2], 16, "%d", 3*  PG_GETARG_INT32(0));

        /* build a tuple */
        tuple = BuildTupleFromCStrings(attinmeta, values);

        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else    /* do when there is no more left */
    {
        SRF_RETURN_DONE(funcctx);
    }
}
