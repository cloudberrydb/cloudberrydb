/*-------------------------------------------------------------------------
 *
 * execMain.c
 *	  top level executor interface routines and generate arrow expression tree.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/vecexecutor/execMain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_operator_d.h"
#include "cdb/cdbvars.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "parser/scansup.h"
#include "optimizer/optimizer.h"
#include "utils/fmgr_vec.h"
#include "utils/lsyscache.h"
#include "utils/tuptable_vec.h"
#include "utils/vecfuncs.h"
#include "utils/vecsort.h"
#include "vecexecutor/execslot.h"
#include "vecexecutor/executor.h"

typedef struct VecAggInfo
{
	Aggref *aggref;
	WindowFunc *wfunc;
	const char *aname; /* arrow aggregate function name */
	GArrowFunctionOptions *options;
	char inname[NAMEDATALEN + 30];
	char outname[NAMEDATALEN + 30];
} VecAggInfo;

typedef enum
{
	Plain,
	Orderby,
	Selectk,
	Mergesort,
	Consuming,
} SinkType;

typedef struct PlanBuildContext
{
	GArrowExecutePlan *plan;
	PlanState *planstate;

	/* Aggregate information */
	List *agginfos;
	AggSplit aggsplit;
	AggStrategy aggstrategy;
	const gchar **keys; /* column name of group keys, used by windowagg also */
	int nkey;           /* number of groups, used by windowagg also */
	bool ishaving;

	/* windowagg fields */
	GArrowSortOptions *orderby_sortoption;

	/* schema of plan input batch*/
	GArrowSchema *inputschema;

	/* plan recipe */
	SinkType sinktype;
	int nlimit; /* the "k" of selectk */

	int *map;/* scan mapping col*/

	/* reader */
	bool pipeline;
	GArrowRecordBatchReader *reader;

	/* hashjoin related */
	bool is_hashjoin;
	bool is_hashjoin_after_node;
	bool is_left_schema;
	/* left and right raw schema */
	GArrowSchema *left_in_schema;
	GArrowSchema *right_in_schema;
	/* left and right proj schema */
	GArrowSchema *left_proj_schema;
	GArrowSchema *right_proj_schema;

	GList *left_hashkeys;
	GList *right_hashkeys;

	/* nestloopjoin related */
	bool is_nestloopjoin;

	/* CaseTestExpr is case when placeholder */
	Expr *case_test_expr;
	bool is_case_when;
	GArrowExpression *whenexpr;
	GArrowExpression *not_and_whenexpr;
	Oid case_when_type;
} PlanBuildContext;

typedef struct SortKey
{
	GArrowSortOrder *orders;
	GArrowSortOrder *nulls_first;
} SortKey;

GArrowSchema *dummy_schema = NULL;
static inline GArrowExpression *func_args_to_expression(List *args,
		PlanBuildContext *pcontext, const char *name);
static inline VecAggInfo* new_agg_info(Aggref *aggref, int aggno,
		PlanBuildContext *pcontext);
static inline VecAggInfo* new_winagg_info(WindowFunc *wfunc, PlanBuildContext *pcontext);
static GArrowExpression *expr_to_arrow_expression(Expr *node,
		PlanBuildContext *pcontext);
static const char* get_agg_func_name(Aggref *aggref, const FuncTable *table,
		PlanBuildContext *pcontext);
static GArrowExecuteNode* BuildSource(PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildProject(List *targetList, List *qualList,
									   GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void BuildSink(GArrowExecuteNode *input, VecExecuteState *estate, PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildAggregatation(List *aggInfos,
											 GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void *get_scan_next_batch(PlanState *node);
static void *get_foreign_next_batch(PlanState *node);
static void *get_current_next_batch(PlanState *node);
static GList* build_sort_keys(PlanState *planstate, GArrowSchema *schema,
		PlanBuildContext *pcontext);
static GArrowProjectNodeOptions* build_project_options(List *targetList,
		PlanBuildContext *pcontext);
static GArrowProjectNodeOptions* build_agg_project_options(List *targetList, List *aggInfos,
		PlanBuildContext *pcontext);
static GArrowProjectNodeOptions* build_winagg_project_options(List *targetList, List *aggInfos,
		PlanBuildContext *pcontext);
static GArrowAggregateNodeOptions* build_aggregatation_options(GList *aggregations,
		PlanBuildContext *pcontext);

static GArrowFilterNodeOptions *build_filter_options(List *filterInfo,
													 PlanBuildContext *pcontext);
static GArrowExpression *
replace_substring_regex_expression(List *args, PlanBuildContext *pcontext);
static GArrowExpression *
get_text_like_expression(OpExpr *opexpr, PlanBuildContext *pcontext);
static GArrowExpression* replace_expression(List *args,
				 PlanBuildContext *pcontext);
static GArrowExpression *
extract_expression(List *args, PlanBuildContext *pcontext, bool retnumeric);
static  GArrowExpression *
int4_to_numeric_expression(List *args, PlanBuildContext *pcontext);
static GArrowExpression *
utf8_slice_codeunits_expression(List *args, PlanBuildContext *pcontext);
static GArrowExecuteNode *BuildHashjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual);
static GArrowExecuteNode *BuildJoinProject(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static GArrowProjectNodeOptions *build_join_project_options(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext);
static void rewrite_tl_keys(List *targetList, PlanBuildContext *pcontext);
static GArrowExpression *build_cast_expression(FuncExpr *opexpr, PlanBuildContext *pcontext);
static void get_windowagg_sortorder(PlanBuildContext *pcontext, SortKey *sortKey);
static GArrowExecuteNode *BuildNestLoopjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual);
static void BuildJoinPlan(PlanBuildContext *pcontext, VecExecuteState *estate);
static const char *GetHashJoinProjectName(PlanBuildContext *pcontext, const char *name);
static GArrowExpression *build_text_join(List *args, PlanBuildContext *pcontext);
static GArrowExpression *build_is_distinct_expression(DistinctExpr *dex, PlanBuildContext *pcontext);
static GArrowExpression *build_null_if_expression(NullIfExpr *dex, PlanBuildContext *pcontext);
static GArrowExecuteNode*
build_orderby_node(PlanState *planstate, GArrowExecutePlan *plan,  GArrowExecuteNode *input, PlanBuildContext *pcontext);
static GArrowExpression *
build_literal_expression(GArrowType type, Datum datum, bool isnull, Oid pg_type);

static PlanState *
get_sequence_exact_result_ps(PlanState *plan)
{
	SequenceState *node = castNode(SequenceState, plan);

	PlanState *lastPS = node->subplans[node->numSubplans - 1];

	if (IsA(lastPS, SequenceState))
	{
		return get_sequence_exact_result_ps(lastPS);
	}

	return lastPS;
}

static const char *
get_agg_func_name(Aggref *aggref, const FuncTable *table, PlanBuildContext *pcontext)
{
	int cols;
	const char *name = NULL;

	cols = list_length(aggref->aggdistinct);

	Assert(table != NULL);

	/* is a hash agg */
	if (pcontext->aggstrategy == AGG_HASHED)
		/* is a hash distinct agg */
		if (cols > 0)
			name = table->hashDistFuncName;
		else 
			name = table->hashFuncName;
	else if (cols > 0) /* is a distinct agg */
		name = table->distFuncName;
	else
		name = table->funcName;

	if (!name)
		elog(ERROR, "Cann't find Arrow agg function name.");
	return name;
}

const char *
get_op_name(Oid opno)
{
	char *name = get_opname(opno);
	if (!name)
		return NULL;
	if (0 == strcmp(name, "+"))
		return "add";
	else if (0 == strcmp(name, "-"))
		return "subtract";
	else if (0 == strcmp(name, "*"))
		return "multiply";
	else if (0 == strcmp(name, "/"))
		return "divide";
	else if (0 == strcmp(name, ">"))
		return "greater";
	else if (0 == strcmp(name, ">="))
		return "greater_equal";
	else if (0 == strcmp(name, "<"))
		return "less";
	else if (0 == strcmp(name, "<="))
		return "less_equal";
	else if (0 == strcmp(name, "="))
		return "equal";
	else if (0 == strcmp(name, "<>"))
		return "not_equal";
	else if (0 == strcmp(name, "~~"))
	{
		/* doesn't support regex like: substring(c_phone, '([0-9]{1,4})'); */
		if (opno == F_SUBSTRING_TEXT_TEXT || opno == F_SUBSTRING_TEXT_TEXT_TEXT)
			return NULL;
		return "match_like";
	}
	else if(0 == strcmp(name, "!~~"))
		return "match_not_like";
	else if(0 == strcmp(name, "||"))
		return "binary_join_element_wise";
	return NULL;
}

/*
 * Fixme: Is used for fallback now, should also be
 * the converter of funcion.
 * Merge with get_function_expression.
 * Get function by static table based on Oid.
 */
const char *
get_function_name(Oid opno)
{
	const char *name = get_func_name(opno);
	if (!name)
		return NULL;
	else if (0 == strcmp(name, "text"))
		return "text";
	else if (0 == strcmp(name, "length"))
		return "utf8_length";
	else if (0 ==strcmp(name, "date_trunc"))
		return "strptime";
	else if (0 == strcmp(name, "regexp_replace"))
		return "dummy";
	else if (0 == strcmp(name, "replace"))
		return "replace_substring";
	else if (0 == strcmp(name, "extract"))
		return "dummy";
	else if (0 == strcmp(name, "numeric") && opno == F_NUMERIC_INT4)
		return "dummy";
	else if (0 == strcmp(name, "substring") || 0 == strcmp(name, "substr"))
		return "dummy";
	else if (0 == strcmp(name, "abs"))
		return "abs";
	else if (0 == strcmp(name, "upper"))
		return "upper";
	else if (0 == strcmp(name, "boolne"))
		return "dummy";
	else if (0 == strcmp(name, "float8") || 0 == strcmp(name, "int4"))
		return "dummy";
	else if (0 == strcmp(name, "round"))
		return "dummy";
	return NULL;
}

/*
 * New an arrow expression with PG function arguments and arrow function name.
 * such as: 
 *  i + j - 1 will generate subtract(add(i, j), 1)
 */
static inline GArrowExpression *
func_args_to_expression(List *args, PlanBuildContext *pcontext, const char* funcname)
{
	ListCell *l;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression)  result_expr = NULL;

	foreach(l, args)
	{
		Expr  *fle = (Expr *) lfirst(l);
		g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(fle, pcontext);

		if (!expr)
		{
			elog(ERROR, "Expression to arrow error.");
			return NULL;
		}

		/* No more than two arguments for arrow function.
		 * Combine the existing two arguments as a new arrow function.
		 */
		if (g_list_length(arguments) >= 2)
		{
			g_autoptr(GArrowExpression) funcexpr = NULL;

			funcexpr = GARROW_EXPRESSION(garrow_call_expression_new(
					funcname, arguments, NULL));
			if (!funcexpr)
					elog(ERROR, "Failed to new arrow call expression %s.", funcname);
			garrow_list_free_ptr(&arguments);
			arguments = garrow_list_append_ptr(arguments, funcexpr);
		}

		arguments = garrow_list_append_ptr(arguments, expr);
	}

	result_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			funcname, arguments, NULL));
	if (!result_expr)
			elog(ERROR, "Failed to new arrow call expression %s.", funcname);
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(result_expr);
}

static GArrowExpression *
build_round_expr(List *args, PlanBuildContext *pcontext)
{
	ListCell *l = NULL;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) round_expr = NULL;
	g_autoptr(GArrowRoundOptions) options = NULL;
	int pos = 0;
	foreach (l, args)
	{
		g_autoptr(GArrowExpression) cur_expr = NULL;
		Expr  *expr = (Expr *) lfirst(l);
		if (IsA(expr, Const)){
			Const* const_expr = (Const*) expr;
			pos = const_expr->constvalue;
			continue;
		}
		cur_expr = expr_to_arrow_expression(expr, pcontext);
		arguments = garrow_list_append_ptr(arguments, cur_expr);
	}
	options = garrow_round_options_new();
	garrow_round_options_set(options, GARROW_ROUND_HALF_TO_EVEN, pos);
	round_expr = GARROW_EXPRESSION(garrow_call_expression_new("round", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(round_expr);
}

static GArrowExpression *
build_text_join(List *args, PlanBuildContext *pcontext)
{
	ListCell *l = NULL;
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) text_join_expr = NULL;
	// add empty separator to impl text join
	g_autoptr(GArrowDatum) val_datum = NULL;
	g_autoptr(GArrowExpression) li_expr = NULL;
	g_autoptr(GArrowBuffer) empty_buffer = NULL;
	g_autoptr(GArrowScalar) empty_buffer_scalar = NULL;
	foreach (l, args)
	{
		g_autoptr(GArrowExpression) cur_expr = NULL;
		Expr *arg = (Expr *) lfirst(l);
		cur_expr = expr_to_arrow_expression(arg, pcontext);
		arguments = garrow_list_append_ptr(arguments, cur_expr);
	}
	empty_buffer = garrow_buffer_new((const guint8*)(""), 0);
	empty_buffer_scalar =GARROW_SCALAR(garrow_string_scalar_new(empty_buffer));
	val_datum = GARROW_DATUM(garrow_scalar_datum_new(empty_buffer_scalar));
	li_expr = GARROW_EXPRESSION(garrow_literal_expression_new(val_datum));
	arguments = garrow_list_append_ptr(arguments, li_expr);
	text_join_expr = GARROW_EXPRESSION(garrow_call_expression_new("binary_join_element_wise", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(text_join_expr);
}

static GArrowExpression *
build_is_in(GArrowExpression *expr, GArrowDatum *value_set, gboolean skip_nulls)
{
	g_autoptr(GArrowSetLookupOptions) options = NULL;
	g_autoptr(GArrowExpression) is_in_expr = NULL;
	GList *arguments = NULL;
	options = garrow_set_lookup_options_new(value_set, skip_nulls);
	arguments = garrow_list_append_ptr(arguments, expr);
	is_in_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_in", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_in_expr);
}

static inline GArrowExpression *
func_arg_to_expression(Expr *fle, PlanBuildContext *pcontext, const char* funcname)
{
	g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(fle, pcontext);
	GList		*arguments = NULL;

	if (expr)
	{
		arguments = garrow_list_append_ptr(arguments, expr);
		expr = GARROW_EXPRESSION(garrow_call_expression_new(funcname, arguments, NULL));
		garrow_list_free_ptr(&arguments);
		return garrow_move_ptr(expr);
	}
	else
	{
		elog(ERROR, "Expression to arrow error.");
		return NULL;
	}
}

static GArrowExpression *
get_function_expression(FuncExpr *opexpr, PlanBuildContext *pcontext)
{
	Oid opno = opexpr->funcid;

	const char *name = get_func_name(opno);
	if (!name)
	{
		elog(ERROR, "Failed to call get_function_expression(%d) ", opno);
		return NULL;
	}
	else if (0 == strcmp(name, "length"))
		return func_args_to_expression(opexpr->args, pcontext, "utf8_length");
	else if (0 == strcmp(name, "float8") || 0 == strcmp(name, "int4"))
	{
		return build_cast_expression(opexpr, pcontext);
	}
	else if (0 ==strcmp(name, "date_trunc"))
		return func_args_to_expression(opexpr->args, pcontext, "strptime");
	else if (0 == strcmp(name, "regexp_replace"))
		return replace_substring_regex_expression(opexpr->args, pcontext);
	else if (0 == strcmp(name, "replace"))
		return replace_expression(opexpr->args, pcontext);
    /* Fixme: Should have been NUMERICOID .*/
	else if (0 == strcmp(name ,"extract"))
		return extract_expression(opexpr->args, pcontext, opexpr->funcresulttype == FLOAT8OID);
	else if (0 == strcmp(name, "numeric") && opexpr->funcid == F_NUMERIC_INT4)
		return int4_to_numeric_expression(opexpr->args, pcontext);
	else if (0 == strcmp(name, "substring") || 0 == strcmp(name, "substr")) 
		return utf8_slice_codeunits_expression(opexpr->args, pcontext);
	else if (0 == strcmp(name, "int8"))
		return build_cast_expression(opexpr, pcontext);
	else if (0 == strcmp(name, "boolne"))
		return func_args_to_expression(opexpr->args, pcontext, "not_equal");
	else if (0 == strcmp(name, "abs"))
		return func_args_to_expression(opexpr->args, pcontext, "abs");
	else if (0 == strcmp(name, "upper"))
		return func_args_to_expression(opexpr->args, pcontext, "utf8_upper");
	else if (0 == strcmp(name, "round"))
		return build_round_expr(opexpr->args, pcontext);
	else if (0 == strcmp(name, "text"))
		return build_cast_expression(opexpr, pcontext);
	else
		elog(ERROR, "get_function_expression unrecognized typeid: %d funname: %s", opno, name);
	return NULL;
}

static GArrowExpression *
build_cast_expression(FuncExpr *opexpr, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowCastOptions) cast_options = NULL;
	g_autoptr(GArrowDataType) to_type = NULL;
	g_autoptr(GList) cast_args = NULL;
	g_autoptr(GArrowExpression) cast_to_expr = NULL;
	bool allow_truncate = false;
	Oid opno = opexpr->funcid;

	Assert(list_length(opexpr->args) == 1);
	expr = expr_to_arrow_expression(linitial(opexpr->args), pcontext);

	switch (opno)
	{
		case F_INT2_NUMERIC:
		case F_INT4_NUMERIC:
		case F_INT8_NUMERIC:
		case F_FLOAT4_NUMERIC:
		case F_FLOAT8_NUMERIC:
		case F_INT8_INT2:
		case F_INT8_INT4:
		case F_FLOAT8_FLOAT4:
			return garrow_move_ptr(expr);
		case F_FLOAT8_INT2:
		case F_FLOAT8_INT4:
		case F_FLOAT8_INT8:
		{
			cast_args = garrow_list_append_ptr(cast_args, expr);
			to_type = GARROW_DATA_TYPE(garrow_double_data_type_new());
			break;
		}
		case F_INT4_FLOAT8:
		{
			cast_args = garrow_list_append_ptr(cast_args, expr);
			to_type = GARROW_DATA_TYPE(garrow_int32_data_type_new());
			allow_truncate = true;
			break;
		}
		case F_INT4_INT8:
		{
			cast_args = garrow_list_append_ptr(cast_args, expr);
			to_type = GARROW_DATA_TYPE(garrow_int32_data_type_new());
			break;
		}
		case F_TEXT_BPCHAR:
		{
			cast_args = garrow_list_append_ptr(cast_args, expr);
			to_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
			break;
		}
		default:
			break;
	}
	if (!to_type) 
		elog(ERROR, "build explict cast type is error.");
	cast_options = GARROW_CAST_OPTIONS(g_object_new(GARROW_TYPE_CAST_OPTIONS, "to-data-type", garrow_move_ptr(to_type), NULL));
	if (allow_truncate)
		garrow_set_cast_options(cast_options, GARROW_PROP_ALLOW_FLOAT_TRUNCATE, true);
	cast_to_expr = GARROW_EXPRESSION(garrow_call_expression_new("cast", cast_args, GARROW_FUNCTION_OPTIONS(garrow_move_ptr(cast_options))));
	garrow_list_free_ptr(&cast_args);
	return garrow_move_ptr(cast_to_expr);
} 

/*
 * Convert ScalarArrayOpExpr to GArrowExpression
 *
 * Fixme: Fully support of ScalarArrayOpExpr, only "IN" now.
 */
static GArrowExpression *
scalararray_to_expression(ScalarArrayOpExpr *arrayexpr, PlanBuildContext *pcontext)
{
	Oid			elemtype;
	Const *const_expr;
	Expr *second_expr;
	g_autoptr(GArrowArray) array = NULL;
	g_autoptr(GArrowArrayDatum) array_datum = NULL;
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowScalar) val_scalar = NULL;
	g_autoptr(GArrowDatum) val_datum = NULL;

	second_expr = (Expr *) lsecond(arrayexpr->args);
	if (nodeTag(second_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}
	/* FIXME: Only support equal operator */
	Assert(!strcmp(get_opname(arrayexpr->opno), "="));
	const_expr = (Const *) second_expr;
	/* first GArrowExpression as is_in argument */
	expr = expr_to_arrow_expression(linitial(arrayexpr->args), pcontext);
	Assert(expr);
	if (const_expr->constisnull)
	{
		val_scalar = ArrowScalarNew(GARROW_TYPE_NA,
									const_expr->constvalue, const_expr->consttype);
	}
	elemtype = get_element_type(const_expr->consttype);
	if (elemtype == InvalidOid)
	{
		elog(ERROR, "is_in right is not an array.");
	}
	if (!const_expr->constisnull)
		val_scalar = ArrowScalarNew(PGTypeToArrowID(const_expr->consttype),
									const_expr->constvalue, const_expr->consttype);
	array = garrow_base_list_scalar_get_value(GARROW_BASE_LIST_SCALAR(val_scalar));
	array_datum = garrow_array_datum_new(array);
	/* is_in need skip null follow pg(in null is false) */
	return build_is_in(garrow_move_ptr(expr), GARROW_DATUM(array_datum), true);
}

static GArrowExpression *
build_not_expression(GArrowExpression *expr)
{
	g_autoptr(GArrowExpression) not_expr = NULL;
	GList *arguments = NULL;
	arguments = garrow_list_append_ptr(arguments, expr);
	not_expr = GARROW_EXPRESSION(garrow_call_expression_new("invert", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(not_expr);
}

static GArrowExpression *
build_is_not_null(GArrowExpression *expr)
{

	GList *arguments = NULL;
	g_autoptr(GArrowExpression) is_not_null_expr = NULL;
	arguments = garrow_list_append_ptr(arguments, expr);
	is_not_null_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_valid", arguments, NULL));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_not_null_expr);
}

// NaN values can also be considered null by setting NullOptions::nan_is_null.
static GArrowExpression *
build_is_null(GArrowExpression *expr, gboolean nan_is_null)
{
	GList *arguments = NULL;
	g_autoptr(GArrowNullOptions) options = NULL;
	g_autoptr(GArrowExpression) is_null_expr = NULL;
	arguments = garrow_list_append_ptr(arguments, expr);
	options = garrow_null_options_new(nan_is_null);
	is_null_expr = GARROW_EXPRESSION(garrow_call_expression_new("is_null", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_null_expr);
}

/*
 * Convert NullTest to GArrowExpression
 */
static GArrowExpression *
nulltest_to_expression(NullTest *node, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression) expr = NULL;
	expr = expr_to_arrow_expression(node->arg, pcontext);
	if (!expr)
	{
		return NULL;
	}
	if (node->nulltesttype == IS_NULL)
	{
		return build_is_null(expr, true);
	}
	else
	{
		return build_is_not_null(expr);
	}
}

/*
 * Convert CaseExpr to GArrowExpression
 * format is:
 *  call("case_when", {
 * 		call("make_struct",
 * 			 {call("greater", {field_ref("a"), literal(1)})},
 * 			 MakeStructOptions({"case_0"})),
 * 		literal(1),
 * 		literal(2),
 *	});
 */
static GArrowExpression *
caseexpr_to_expression(CaseExpr* node, PlanBuildContext *pcontext)
{
	ListCell   *l;
	GList *when_arguments = NULL;
	GList *expr_arguments = NULL;
	GList *and_arguments = NULL;
	GList *not_arguments = NULL;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) default_expr;
	g_autoptr(GArrowExpression) expr;
	g_autoptr(GArrowExpression) and_condition_expr = NULL;
	g_autoptr(GArrowMakeStructOptions) options;
	int i = 0;
	gchar **fields;
	fields = palloc(list_length(node->args) * (sizeof(gchar *)));
	pcontext->case_test_expr = node->arg;
	pcontext->is_case_when = true;
	pcontext->whenexpr = NULL;
	pcontext->not_and_whenexpr = NULL;
	pcontext->case_when_type = exprType((Node *) node);
	foreach (l, node->args)
	{
		CaseWhen *when;
		g_autoptr(GArrowExpression) whenexpr;
		g_autoptr(GArrowExpression) whenexpr_copy;
		g_autoptr(GArrowExpression) resexpr;
		when = lfirst_node(CaseWhen, l);
		whenexpr = expr_to_arrow_expression(when->expr, pcontext);
		pcontext->whenexpr = expr_to_arrow_expression(when->expr, pcontext);
		whenexpr_copy = expr_to_arrow_expression(when->expr, pcontext);
		if (list_length(node->args) >= 2)
		{
			and_arguments = garrow_list_append_ptr(and_arguments, whenexpr_copy);
		}
		else
		{
			and_condition_expr = garrow_move_ptr(whenexpr_copy);
		}
		resexpr = expr_to_arrow_expression(when->result, pcontext);
		when_arguments = garrow_list_append_ptr(when_arguments, whenexpr);
		expr_arguments = garrow_list_append_ptr(expr_arguments, resexpr);
		fields[i] = palloc(sizeof(int) + 6);
		snprintf(fields[i], sizeof(int) + 6, "case_%d", i);
		i++;
	}

	if (list_length(node->args) >= 2)
		and_condition_expr = GARROW_EXPRESSION(garrow_call_expression_new("and_kleene", and_arguments, NULL));

	not_arguments = garrow_list_append_ptr(not_arguments, and_condition_expr);
	pcontext->not_and_whenexpr = GARROW_EXPRESSION(garrow_call_expression_new("invert", not_arguments, NULL));
	options = garrow_make_struct_options_new((const gchar **) fields, i);
	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	default_expr = expr_to_arrow_expression(node->defresult, pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, default_expr);
	expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));
	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	if (and_arguments) 
		garrow_list_free_ptr(&and_arguments);
	garrow_list_free_ptr(&not_arguments);
	ARROW_FREE(GArrowExpression, &pcontext->not_and_whenexpr);
	ARROW_FREE(GArrowExpression, &pcontext->whenexpr);
	pcontext->is_case_when = false;
	pcontext->not_and_whenexpr = NULL;
	pcontext->whenexpr = NULL;
	return garrow_move_ptr(expr);
}

static GArrowExpression *
build_divide_casewhen(Expr *expr, PlanBuildContext *pcontext)
{
	GList *when_arguments = NULL;
	GList *expr_arguments = NULL;
	g_autoptr(GArrowExpression) casewhen_expr;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) default_expr;
	g_autoptr(GArrowMakeStructOptions) options;
	gchar **fields;
	fields = palloc((sizeof(gchar *)));
	g_autoptr(GArrowExpression) resexpr;
	resexpr = expr_to_arrow_expression(expr, pcontext);
	if (pcontext->not_and_whenexpr)
	{
		when_arguments = garrow_list_append_ptr(when_arguments, pcontext->not_and_whenexpr);
	}
	else
	{
		when_arguments = garrow_list_append_ptr(when_arguments, pcontext->whenexpr);
	}
	expr_arguments = garrow_list_append_ptr(expr_arguments, resexpr);
	fields[0] = palloc(sizeof(int) + 6);
	snprintf(fields[0], sizeof(int) + 6, "case_%d", 0);
	options = garrow_make_struct_options_new((const gchar **) fields, 1);
	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	default_expr = build_literal_expression(GARROW_TYPE_NA, 0, true, pcontext->case_when_type);
	expr_arguments = garrow_list_append_ptr(expr_arguments, default_expr);
	casewhen_expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));
	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(casewhen_expr);
}


static GArrowExpression*
build_divide_casewhen_opexpr(OpExpr *opexpr, PlanBuildContext *pcontext)
{
	Expr *divisor = (Expr *) linitial(opexpr->args);
	Expr *dividend = (Expr *) lsecond(opexpr->args);
	g_autoptr(GArrowExpression) expr;
	g_autoptr(GArrowExpression) divisor_expr;
	g_autoptr(GArrowExpression) case_when_expr;
	GList *expr_arguments = NULL;
	divisor_expr = expr_to_arrow_expression(divisor, pcontext);
	case_when_expr = build_divide_casewhen(dividend, pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, divisor_expr);
	expr_arguments = garrow_list_append_ptr(expr_arguments, case_when_expr);
	expr = GARROW_EXPRESSION(garrow_call_expression_new("divide", expr_arguments, NULL));
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(expr);
}

static GArrowExpression *
extract_expression(List *args, PlanBuildContext *pcontext, bool retnumeric)
{
	ListCell   *lc;
	int type, val;
	char	   *lowunits = NULL;
	type = UNKNOWN_FIELD;
	foreach(lc, args)
	{
		Expr	   *arg = (Expr *) lfirst(lc);

		if (IsA(arg, Const))
		{
			Const	   *con = (Const *) arg;
			text	   *units = DatumGetTextPP(con->constvalue);

			lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
													VARSIZE_ANY_EXHDR(units),
														false);
			type = DecodeUnits(0, lowunits, &val);
		}
		else
		{
			if (type == UNITS)
			{
				g_autoptr(GArrowExpression) extract_expr = NULL;
				switch (val)
				{
					case DTK_MICROSEC:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_microsecond");
						break;
					case DTK_MILLISEC:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_millisecond");
						break;
					case DTK_SECOND:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_second");
						break;
					case DTK_MINUTE:
						extract_expr = func_arg_to_expression(arg, pcontext, "minute");
						break;
					case DTK_HOUR:
						extract_expr = func_arg_to_expression(arg, pcontext, "hour");
						break;
					case DTK_DAY:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_day");
						break;
					case DTK_MONTH:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_month");
						break;
					case DTK_YEAR:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_year");
						break;
					case DTK_DECADE:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_decade");
						break;
					case DTK_CENTURY:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_century");
						break;
					case DTK_MILLENNIUM:
						extract_expr = func_arg_to_expression(arg, pcontext, "pg_millennium");
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("timestamp units  not supported")));
				}
				// FIXME: will be use in the future.
				// if(retnumeric){
				// 	// add cast expr(integer to decimal) to align pg extract expr output type
				// 	g_autoptr(GArrowCastOptions)  cast_options = NULL;
				// 	g_autoptr(GArrowDataType) to_type = NULL;
				// 	g_autoptr(GList) cast_args = NULL;
				// 	g_autoptr(GArrowExpression) cast_decimal_expr = NULL;
				// 	to_type = GARROW_DATA_TYPE(garrow_decimal128_data_type_new(garrow_decimal128_data_type_max_precision(), 0));
				// 	cast_args = garrow_list_append_ptr(cast_args, extract_expr);
				// 	cast_options = GARROW_CAST_OPTIONS(g_object_new(GARROW_TYPE_CAST_OPTIONS, "to-data-type", garrow_move_ptr(to_type) , NULL));
				// 	cast_decimal_expr = GARROW_EXPRESSION(garrow_call_expression_new("cast", garrow_move_ptr(cast_args), GARROW_FUNCTION_OPTIONS(garrow_move_ptr(cast_options))));
				// 	return garrow_move_ptr(cast_decimal_expr);
				// }
				// add cast expr(integer to decimal) to align pg extract expr output type
				g_autoptr(GArrowCastOptions)  cast_options = NULL;
				g_autoptr(GArrowDataType) to_type = NULL;
				g_autoptr(GList) cast_args = NULL;
				g_autoptr(GArrowExpression) cast_double_expr = NULL;
				to_type = GARROW_DATA_TYPE(garrow_double_data_type_new());
				cast_args = garrow_list_append_ptr(cast_args, extract_expr);
				cast_options = GARROW_CAST_OPTIONS(g_object_new(GARROW_TYPE_CAST_OPTIONS, "to-data-type", garrow_move_ptr(to_type) , NULL));
				cast_double_expr = GARROW_EXPRESSION(garrow_call_expression_new("cast", garrow_move_ptr(cast_args), GARROW_FUNCTION_OPTIONS(garrow_move_ptr(cast_options))));
				return garrow_move_ptr(cast_double_expr);
			}
			else
			{
				elog(ERROR, "Failed to call extract type (%d) ", type);
				return NULL;
			}

		}
	}
	elog(ERROR, "Failed to call extract_expression");
	return NULL;
}

static GArrowExpression *
int4_to_numeric_expression(List *args, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression) expr = NULL;
	Expr *var_expr = NULL;

	var_expr = (Expr *) linitial(args);
	if (nodeTag(var_expr) != T_Var)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(var_expr));
	}

	expr = expr_to_arrow_expression(var_expr, pcontext);
	return garrow_move_ptr(expr);
}

static GArrowExpression *
utf8_slice_codeunits_expression(List *args, PlanBuildContext *pcontext)
{
	Expr *inner_expr = NULL;
	Expr *start_expr = NULL;
	Expr *len_expr = NULL;
	Const *const_expr_start = NULL;
	Const *const_expr_end = NULL;
	int start = 0, len = 0, l1 = 0, end = INT_MAX;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) slice_expression = NULL;
	g_autoptr(GArrowSliceOptions) options = NULL;
	GList *arguments = NULL;
	inner_expr = (Expr *) linitial(args);
	expr = expr_to_arrow_expression(inner_expr, pcontext);
	Assert(expr);
	arguments = garrow_list_append_ptr(arguments, expr);
	start_expr = (Expr *) lsecond(args);
	/* FIXME: will fallback in the planner. */
	if (nodeTag(start_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(start_expr));
	}
	const_expr_start =  (Const *) start_expr;
	if (const_expr_start->constbyval)
	{
		start = const_expr_start->constvalue;
		if (start < 1)
		{
			l1 = start - 1;
			start = 1;
		}
		start--;
	}
	/* regex plan fallback */
	if (list_length(args) == 3) {
		len_expr = (Expr *) lthird(args);
		if (nodeTag(len_expr) != T_Const)
		{
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(len_expr));
		}
		const_expr_end =  (Const *) len_expr;
		len = const_expr_end->constvalue;
		if (len < 0)
		{
			elog(ERROR, "negative substring length not allowed");
		}
		end = start + len + l1;
	}
	options = garrow_slice_options_new(start, end, 1);
	slice_expression = GARROW_EXPRESSION(garrow_call_expression_new(
		"utf8_slice_codeunits", arguments, GARROW_FUNCTION_OPTIONS(options)));
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(slice_expression);
}

static GArrowExpression *
replace_substring_regex_expression(List *args, PlanBuildContext *pcontext)
{
	Expr *first_expr = NULL;
	Expr *second_expr = NULL;
	Expr *third_expr = NULL;
	Const *const_expr_pattern = NULL;
	Const *const_expr_replace = NULL;
	char *str_pattern = NULL;
	char *str_replace = NULL;
	struct varlena *s = NULL;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) replace_expression = NULL;
	g_autoptr(GArrowReplaceSubstringOptions) options = NULL;
	GList *arguments = NULL;

	first_expr = (Expr *) linitial(args);
	if (nodeTag(first_expr) != T_Var)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(first_expr));
	}
	expr = expr_to_arrow_expression(first_expr, pcontext);
	Assert(expr);

	second_expr = (Expr *) lsecond(args);
	if (nodeTag(second_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}
	const_expr_pattern =  (Const *) second_expr;

	third_expr = (Expr *) lthird(args);
	if (nodeTag(third_expr) != T_Const)
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(third_expr));
	}
	const_expr_replace =  (Const *) third_expr;
	s = (struct varlena *) const_expr_pattern->constvalue;
	str_pattern = text_to_cstring(s);
	s = (struct varlena *) const_expr_replace->constvalue;
	str_replace = text_to_cstring(s);

	options = garrow_replace_substring_options_new(str_pattern, str_replace, -1);
	arguments = garrow_list_append_ptr(arguments, expr);
	replace_expression = GARROW_EXPRESSION(garrow_call_expression_new(
			"replace_substring_regex", arguments, GARROW_FUNCTION_OPTIONS(options)));

	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(replace_expression);
}

static GArrowExpression *
get_text_like_expression(OpExpr *opexpr, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression)  result_expr = NULL;
	gboolean ignore_case = false;
	GList           *arguments = NULL;

	Expr  *first_expr = linitial(opexpr->args);
	Expr  *second_expr = lsecond(opexpr->args);

	Const *con = (Const *)second_expr;
	char *value = text_to_cstring(DatumGetTextP(con->constvalue));
	g_autoptr(GArrowMatchSubstringOptions) options =
			garrow_match_substring_options_new(value, ignore_case);
	g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(first_expr, pcontext);

	if (expr)
	{
		arguments = garrow_list_append_ptr(arguments, expr);
		expr = GARROW_EXPRESSION(garrow_call_expression_new(
				"match_like", arguments, GARROW_FUNCTION_OPTIONS(options)));
		garrow_list_free_ptr(&arguments);
		return garrow_move_ptr(expr);
	}
	else
	{
		elog(ERROR, "Expression to arrow error.");
		return NULL;
	}

}

static GArrowExpression *
get_text_not_like_expression(OpExpr *opexpr, PlanBuildContext *pcontext)
{
        g_autoptr(GArrowExpression)  result_expr = NULL;
        gboolean ignore_case = false;
        GList           *arguments = NULL;

        Expr  *first_expr = linitial(opexpr->args);
        Expr  *second_expr = lsecond(opexpr->args);

        Const *con = (Const *)second_expr;
        char *value = text_to_cstring(DatumGetTextP(con->constvalue));
        g_autoptr(GArrowMatchSubstringOptions) options =
        		garrow_match_substring_options_new(value, ignore_case);
        g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(first_expr, pcontext);

        if (expr)
        {
        	g_autoptr(GArrowExpression) not_expr = NULL;
			arguments = garrow_list_append_ptr(arguments, expr);
			expr = GARROW_EXPRESSION(garrow_call_expression_new(
					"match_like", arguments, GARROW_FUNCTION_OPTIONS(options)));
			not_expr = build_not_expression(expr);
			garrow_list_free_ptr(&arguments);
			return garrow_move_ptr(not_expr);
        }
        else
        {
        	elog(ERROR, "Expression to arrow error.");
        	return NULL;
        }
}

static GArrowExpression *
replace_expression(List *args, PlanBuildContext *pcontext)
{
	Expr *first_expr = NULL;
	Expr *second_expr = NULL;
	Expr *third_expr = NULL;
	Const *const_expr_pattern = NULL;
	Const *const_expr_replace = NULL;
	char *str_pattern = NULL;
	char *str_replace = NULL;
	struct varlena *s = NULL;

	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) replace_expression = NULL;
	g_autoptr(GArrowReplaceSubstringOptions) options = NULL;
	GList *arguments = NULL;

	first_expr = (Expr *) linitial(args);
	if (!IsA(first_expr, Var))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(first_expr));
	}
	expr = expr_to_arrow_expression(first_expr, pcontext);
	Assert(expr);

	second_expr = (Expr *) lsecond(args);
	if (!IsA(second_expr,Const))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(second_expr));
	}
	const_expr_pattern =  (Const *) second_expr;

	third_expr = (Expr *) lthird(args);
	if (!IsA(third_expr,Const))
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(third_expr));
	}
	const_expr_replace =  (Const *) third_expr;
	s = (struct varlena *) const_expr_pattern->constvalue;
	str_pattern = text_to_cstring(s);
	s = (struct varlena *) const_expr_replace->constvalue;
	str_replace = text_to_cstring(s);

	options = garrow_replace_substring_options_new(str_pattern, str_replace, -1);
	arguments = garrow_list_append_ptr(arguments, expr);
	replace_expression = GARROW_EXPRESSION(garrow_call_expression_new("replace_substring", arguments, GARROW_FUNCTION_OPTIONS(options)));

	pfree(str_pattern);
	pfree(str_replace);
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(replace_expression);
}

static GArrowExpression *
build_literal_expression(GArrowType type, Datum datum, bool isnull, Oid pg_type)
{
	g_autoptr(GArrowScalar) val_scalar = NULL;
	g_autoptr(GArrowDatum) val_datum = NULL;
	if (isnull)
		val_scalar = ArrowScalarNew(GARROW_TYPE_NA, datum, pg_type);
	else 
		val_scalar = ArrowScalarNew(type, datum, pg_type);
	val_datum = GARROW_DATUM(garrow_scalar_datum_new(val_scalar));
	return GARROW_EXPRESSION(garrow_literal_expression_new(val_datum));
}


static GArrowExpression *
coalesceexpr_expression(CoalesceExpr *coalexpr, PlanBuildContext *pcontext)
{
	GList *arguments = NULL;
	ListCell *l;
	g_autoptr(GArrowExpression)  result_expr = NULL;
	foreach(l, coalexpr->args)
	{
		Expr  *arg_expr = (Expr *) lfirst(l);
		g_autoptr(GArrowExpression) expr = expr_to_arrow_expression(arg_expr, pcontext);

		if (!expr)
		{
			elog(ERROR, "arg expr to arrow error.");
			return NULL;
		}
		arguments = garrow_list_append_ptr(arguments, expr);
	}
	result_expr = GARROW_EXPRESSION(garrow_call_expression_new(
		"coalesce", arguments, NULL));
	if (!result_expr)
		elog(ERROR, "Failed to new arrow call expression coalesce.");
	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(result_expr);
}


static GArrowExpression *
expr_to_arrow_expression(Expr *node, PlanBuildContext *pcontext)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowExpression) expr = NULL;

	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Var:
			{
				const char *attname = NULL;
				Var *var = (Var *) node;

				if (var->varattno == SelfItemPointerAttributeNumber)
				{
					attname = GetCtidSchemaName(pcontext->inputschema);
				}
				else if (var->varattno == GpSegmentIdAttributeNumber)
				{
					expr = build_literal_expression(PGTypeToArrowID(var->vartype), GpIdentity.segindex, false, var->vartype);
					break;
				}
				else if (pcontext->is_nestloopjoin || (pcontext->is_hashjoin && pcontext->is_hashjoin_after_node))
				{
					if (pcontext->inputschema)
						attname = GetSchemaNameByVarNo(pcontext->inputschema, var->varattno, var->varno);
					else 
					{
						Assert(pcontext->left_proj_schema && pcontext->right_proj_schema);
						if (var->varno == OUTER_VAR)
							attname = GetSchemaName(pcontext->left_proj_schema, var->varattno, pcontext->map);
						else 
							attname = GetSchemaName(pcontext->right_proj_schema, var->varattno, pcontext->map);
					}
				} 
				else
					attname = GetSchemaName(pcontext->inputschema, var->varattno, pcontext->map);
				expr = GARROW_EXPRESSION(garrow_field_expression_new(attname, &error));
				if (error)
					elog(ERROR, "convert PG Var(name: %s) to arrow expression failed: %s",
							attname, error->message);
				break;
			}
		case T_Const:
			{
				Const * const_expr = (Const *) node;
				expr = build_literal_expression(PGTypeToArrowID(const_expr->consttype), const_expr->constvalue, const_expr->constisnull, const_expr->consttype);
				break;
			}
		case T_OpExpr:
			{
				OpExpr	  *opexpr = (OpExpr *)node;
				const char  *funcname;
				switch (opexpr->opno)
				{
					case OID_TEXT_LIKE_OP:
						return get_text_like_expression(opexpr, pcontext);
						break;
					case OID_TEXT_NOT_LIKE_OP:
						return get_text_not_like_expression(opexpr, pcontext);
						break;
					case OIDTextConcatenateOperator:
						return build_text_join(opexpr->args, pcontext);
						break;
					default:
						break;
				}
				funcname = get_op_name(opexpr->opno);
				if (!funcname)
					elog(ERROR, "Failed to call get_opname(%d) ", opexpr->opno);
				if (!funcname)
					elog(ERROR, "expression_name unrecognized typeid: %d funname: %s",
							opexpr->opno, funcname);
				if (0 == strcmp(funcname, "divide") && pcontext->is_case_when)
					expr = build_divide_casewhen_opexpr(opexpr, pcontext);
				else
					expr = func_args_to_expression(opexpr->args, pcontext, funcname);
				break;
			}
		case T_WindowFunc:
			{
				WindowFunc *wfunc = (WindowFunc *) node;
				VecAggInfo *agginfo = NULL;
				ListCell *l;

				/* check whether current window agg is duplicated */
				foreach(l, pcontext->agginfos)
				{
					VecAggInfo *cinfo = (VecAggInfo *) lfirst(l);

					if (equal(wfunc, cinfo->wfunc) &&
						!contain_volatile_functions((Node *) wfunc))
					{
						agginfo = cinfo;
					}
				}

				if (!agginfo)
				{
					agginfo = new_winagg_info(wfunc, pcontext);
					pcontext->agginfos = lappend(pcontext->agginfos, agginfo);
				}

				/* winfunc in expression is a normal field for arrow, use out name */
				expr = GARROW_EXPRESSION(garrow_field_expression_new(
						agginfo->outname, &error));
				if (error)
					elog(ERROR, "convert PG winfunc(name: %s) to arrow expression failed: %s",
							agginfo->outname, error->message);

				break;
			}
		case T_Aggref:
			{
				Aggref *aggref = (Aggref *) node;
				VecAggInfo *agginfo = NULL;
				ListCell *l;

				/* check whether current agg is duplicated */
				foreach(l, pcontext->agginfos)
				{
					VecAggInfo *cinfo = (VecAggInfo *) lfirst(l);
					if (cinfo->aggref->aggno == aggref->aggno)
					{
						agginfo = cinfo;
					}
				}

				if (!agginfo)
				{
					agginfo = new_agg_info(aggref,
										   aggref->aggno,
										   pcontext);
					pcontext->agginfos = lappend(pcontext->agginfos, agginfo);
				}
				/* aggref in expression is a normal field for arrow, use out name*/
				expr = GARROW_EXPRESSION(garrow_field_expression_new(
						agginfo->outname, &error));
				if (error)
					elog(ERROR, "convert PG Aggref(name: %s) to arrow expression failed: %s",
							agginfo->outname, error->message);
				break;
			}
		case T_List:
			break;
		case T_FuncExpr:
			{
				FuncExpr	  *opexpr = (FuncExpr *)node;
				return get_function_expression(opexpr, pcontext);
				break;
			}
		case T_RelabelType:
			{
				RelabelType	  *opexpr = (RelabelType *)node;
				return expr_to_arrow_expression(opexpr->arg, pcontext);
			}
		case T_BoolExpr:
			{
				BoolExpr *boolexpr = (BoolExpr *)node;
				const char *funcname;
				switch (boolexpr->boolop)
				{
					case AND_EXPR:
						funcname = "and_kleene"; /* 2 expression */
						break;
					case OR_EXPR:
						funcname = "or_kleene"; /* 2 expression */
						break;
					case NOT_EXPR:
						funcname = "invert"; /* 1 expression */
						break;
					default:
						elog(ERROR, "unrecognized boolop: %d",(int) boolexpr->boolop);
				}

				expr = func_args_to_expression(boolexpr->args, pcontext, funcname);

				break;
			}
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *arrayexpr = (ScalarArrayOpExpr *) node;
				/* FIXME: todo: implement `not in` */
				expr = scalararray_to_expression(arrayexpr, pcontext);				
				break;
			}
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;
				expr = nulltest_to_expression(nt, pcontext);
				break;
			}
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) node;
				expr = caseexpr_to_expression(caseexpr, pcontext);
				break;
			}
		case T_CaseTestExpr:
			{
				Assert(pcontext->case_test_expr);
				expr = expr_to_arrow_expression(pcontext->case_test_expr, pcontext);
				break;
			}
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalexpr = (CoalesceExpr *) node;
				expr = coalesceexpr_expression(coalexpr, pcontext);
				break;
			}
		case T_DistinctExpr:
			{
				DistinctExpr *distinctexpr = (DistinctExpr* ) node;
				expr = build_is_distinct_expression(distinctexpr, pcontext);
				break;
			}
		case T_Param:
			{
				Param* param = (Param *) node;
				ExprContext* exprcontext = pcontext->planstate->ps_ExprContext;
				ParamExecData *prm = &(exprcontext->ecxt_param_exec_vals[param->paramid]);
				expr = build_literal_expression(
					PGTypeToArrowID(param->paramtype), prm->value, prm->isnull, param->paramtype);
				break;
			}
		case T_NullIfExpr:
			{
				NullIfExpr *nullifexpr = (NullIfExpr *) node;
				expr = build_null_if_expression(nullifexpr, pcontext);
				break;
			}

		
		default:
			elog(ERROR, "unrecognized node type: %d", (int)nodeTag(node));
	}
	return garrow_move_ptr(expr);
}

/* generate unique aggname and VecAggInfo*/
static inline VecAggInfo *
new_agg_info(Aggref *aggref, int aggno, PlanBuildContext *pcontext)
{
	VecAggInfo *agginfo;
	const ArrowFmgr *fmgr;
	const FuncTable *table;

	Assert(aggref->aggsplit == pcontext->aggsplit);

	agginfo = palloc(sizeof(VecAggInfo));
	agginfo->aggref = aggref;

	fmgr = get_arrow_fmgr(aggref->aggfnoid);
	if (!fmgr)
		elog(ERROR, "Can not find Arrow aggregate fmgr, aggfnoid: %d",
				aggref->aggfnoid);
	if (pcontext->aggsplit == AGGSPLIT_FINAL_DESERIAL)
		table = get_arrow_functable(fmgr->finalfn);
	else if (pcontext->aggsplit == AGGSPLIT_INITIAL_SERIAL)
		table = get_arrow_functable(fmgr->transfn);
	else if (pcontext->aggsplit == AGGSPLIT_SIMPLE)
		table = get_arrow_functable(fmgr->simplefn);
	else
		elog(ERROR, "doesn't support aggsplit: %d", pcontext->aggsplit);

	agginfo->aname = get_agg_func_name(aggref, table, pcontext);
	agginfo->options = table->getOption(list_length(aggref->args));

	snprintf(agginfo->inname, sizeof(agginfo->inname),
			"_inagg_%d", aggno);
	snprintf(agginfo->outname, sizeof(agginfo->outname),
			"_outagg_%d", aggno);
	return agginfo;
}

/* generate unique aggname and VecAggInfo */
static inline VecAggInfo *
new_winagg_info(WindowFunc *wfunc, PlanBuildContext *pcontext)
{
	VecAggInfo *agginfo;
	const ArrowFmgr *fmgr;
	const FuncTable *table;

	agginfo = palloc(sizeof(VecAggInfo));
	agginfo->wfunc = wfunc;

	fmgr = get_arrow_fmgr(wfunc->winfnoid);
	if (!fmgr)
		elog(ERROR, "Can not find Arrow aggregate fmgr, aggfnoid: %d", wfunc->winfnoid);

	table = get_arrow_functable(fmgr->simplefn);
	agginfo->aname = table->funcName;
	agginfo->options = table->getOption(list_length(wfunc->args));

	int winaggno = list_length(pcontext->agginfos);
	snprintf(agginfo->inname, sizeof(agginfo->inname), "_inwinagg_%d", winaggno);
	snprintf(agginfo->outname, sizeof(agginfo->outname), "_outwinagg_%d", winaggno);

	return agginfo;
}

/* ------------------------------------------------------------------------
 *
 * Build Arrow plan execute state by cbdb planstate.
 *
 * Inputs:
 *   'planstate'is the initialized plan state used to build Arrow plan.
 *
 * Outputs:
 *   'estate' is output execute state, which is used by ExecutePlan.
 *
 * ------------------------------------------------------------------------
 */
void
BuildVecPlan(PlanState *planstate, VecExecuteState *estate)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowExecuteNode) source = NULL;
	List *targetList = planstate->plan->targetlist;
	List *qualList = planstate->plan->qual;
	PlanBuildContext pcontext;

	/* init PlanBuildContext*/
	pcontext.agginfos = NULL;
	pcontext.keys = NULL;
	pcontext.nkey = 0;
	pcontext.planstate = planstate;
	pcontext.reader = NULL;
	pcontext.ishaving = false;
	pcontext.plan = NULL;
	pcontext.map = NULL;
	pcontext.sinktype = Consuming;
	pcontext.pipeline = true;

	pcontext.is_hashjoin = false;
	pcontext.is_nestloopjoin = false;
	pcontext.is_hashjoin_after_node = false;
	pcontext.left_proj_schema = NULL;
	pcontext.right_proj_schema = NULL;
	pcontext.left_hashkeys = NULL;
	pcontext.right_hashkeys = NULL;
	pcontext.inputschema = NULL;
	pcontext.is_case_when = false;
	pcontext.whenexpr = NULL;
	pcontext.not_and_whenexpr = NULL;
	pcontext.case_when_type = InvalidOid;

	/* set build recipe for different plan node */
	switch(nodeTag(planstate))
	{
		case T_HashJoinState:
		{
			pcontext.is_hashjoin = true;
		}
		break;
		case T_NestLoopState:
		{
			pcontext.is_nestloopjoin = true;
		}
		break;
		case T_AggState:
		{
			Agg *agg = (Agg *)planstate->plan;

			if (!outerPlanState(planstate))
				elog(ERROR, "Agg node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
			pcontext.aggstrategy = agg->aggstrategy;
			pcontext.aggsplit = agg->aggsplit;
			pcontext.ishaving = true;
		}
		break;
		case T_WindowAggState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "WindowAgg node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);

			if (DEBUG1 >= log_min_messages)
			{
				g_autofree gchar *str = garrow_schema_to_string(pcontext.inputschema);
				elog(DEBUG1, "input schema for Windowagg: %s", str);
			}
		}
		break;
		case T_ResultState:
		{
			if (!outerPlanState(planstate))
				elog(ERROR, "Result node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
					outerPlanState(planstate)->ps_ResultTupleSlot);
		}
		break;
		case T_SubqueryScanState:
		{
			SubqueryScanState *node = (SubqueryScanState *) planstate;
			if (!node->subplan)
				elog(ERROR, "SubqueryScan node can't be leaf in vector plan");
			pcontext.inputschema = GetSchemaFromSlot(
				node->subplan->ps_ResultTupleSlot);
		}
		break;
		case T_SeqScanState:
		{
			VecSeqScanState *scanstate = (VecSeqScanState *)planstate;
			/* If qualification and projection are both empty,
			 * no need to build plan.
			 *
			 * Todo : project is initialized as ps_ProjInfo for PG first,
			 *		then test here to find whether we need to build project.
			 *		The ps_ProjInfo initialization need to be rewrite for
			 *		arrow plan, to find whether project is need.
			 */
			if ((!planstate->plan->qual) && (!planstate->ps_ProjInfo))
				return;
			pcontext.inputschema = scanstate->vecdesc->schema;
			pcontext.map = scanstate->columnmap;
		}
		break;
		case T_ForeignScanState:
		{
			VecForeignScanState *scanstate = (VecForeignScanState *)planstate;

			if ((!planstate->plan->qual) && (!planstate->ps_ProjInfo))
				return;
			pcontext.inputschema = scanstate->schema;
			pcontext.map = scanstate->columnmap;
		}
		break;
		case T_AssertOpState:
		{
			pcontext.inputschema = GetSchemaFromSlot(outerPlanState(planstate)->ps_ResultTupleSlot);
		}
		break;
		case T_SortState:
		{
			SortState *sortstate = (SortState*)planstate;
			PlanState  *outerNode = outerPlanState(sortstate);
			Sort *sort = (Sort *)planstate->plan;
			if (IsA(outerNode, SequenceState))
				outerNode = get_sequence_exact_result_ps(outerNode);
			pcontext.inputschema = GetSchemaFromSlot(outerNode->ps_ResultTupleSlot);
			pcontext.nkey = sort->numCols;
			pcontext.pipeline = false;
			if (sortstate->bounded)
			{
				pcontext.sinktype = Selectk;
				pcontext.nlimit = sortstate->bound;
			}
			else
			{ 
				pcontext.sinktype = Orderby;
			}
		}
		break;
		default:
			elog(ERROR, "Build arrow plan from (%d) type is not support yet.",
					nodeTag(planstate->plan));
	}

	/* build plan sequentially */
	pcontext.plan = garrow_execute_plan_new(&error);
	if (error)
		elog(ERROR, "Build arrow plan failed: plan node type %d.",
				nodeTag(planstate->plan));

	if (pcontext.is_nestloopjoin)
		BuildJoinPlan(&pcontext, estate);
	else if (pcontext.is_hashjoin)
		BuildJoinPlan(&pcontext, estate);
	else 
	{
		g_autoptr(GArrowExecuteNode) curnode = NULL;
		g_autoptr(GArrowExecuteNode) tmpnode = NULL;
		/* build plan */
		pcontext.is_left_schema = true;
		curnode = BuildSource(&pcontext);
		tmpnode = BuildProject(targetList, qualList, curnode, &pcontext);
		garrow_store_ptr(curnode, tmpnode);

		BuildSink(curnode, estate, &pcontext);
	}

	if(!garrow_execute_plan_validate(pcontext.plan, &error))
		elog(ERROR, "Built invalid arrow plan: %s", error->message);
	
	/* set plan execute state */
	estate->started = false;
	estate->slot = planstate->ps_ResultTupleSlot;
	garrow_store_ptr(estate->plan, pcontext.plan);

	/* read from result queue for consuming sink */
	estate->pipeline = pcontext.pipeline;
	if (pcontext.pipeline)
		estate->resqueue = NIL;
	else
		garrow_store_ptr(estate->reader, pcontext.reader);

	if (DEBUG1 >= log_min_messages)
	{
		g_autofree gchar *str = garrow_execute_plan_to_string(estate->plan);
		elog(DEBUG1, "arrow plan: %s", str);
	}
}

static void 
BuildJoinPlan(PlanBuildContext *pcontext, VecExecuteState *estate)
{
	g_autoptr(GArrowExecuteNode) left_source_node = NULL;
	g_autoptr(GArrowExecuteNode) right_source_node = NULL;
	g_autoptr(GArrowExecuteNode) join_node = NULL;
	g_autoptr(GArrowExecuteNode) left_proj_node = NULL;
	g_autoptr(GArrowExecuteNode) right_proj_node = NULL;
	g_autoptr(GArrowExecuteNode) proj_node = NULL;
	GList *left_fields = NULL;
	GList *right_fields = NULL;
	List *targetList = pcontext->planstate->plan->targetlist;
	List *qualList = pcontext->planstate->plan->qual;
	List *joinqual = NIL;
	List *outerHashKeys = NIL;
	List *innerHashKeys = NIL;
	List *mergeQual = NIL;
	PlanState *planstate = pcontext->planstate;
	if (pcontext->is_nestloopjoin)
	{
		NestLoopState *nlstate = (NestLoopState *) pcontext->planstate;
		if (nlstate->js.joinqual)
			joinqual = (List *) nlstate->js.joinqual->expr;
	}
	else 
	{
		HashJoinState *hjstate = (HashJoinState *) pcontext->planstate;
		HashState *hstate = (HashState *) innerPlanState(pcontext->planstate);
		if (hjstate->js.joinqual)
			joinqual = (List *) hjstate->js.joinqual->expr;
		outerHashKeys = hjstate->hj_OuterHashKeys;
		innerHashKeys = hstate->hashkeys;
	}


	/* build left tree */
	pcontext->inputschema = GetSchemaFromSlot(outerPlanState(planstate)->ps_ResultTupleSlot);
	left_fields = garrow_schema_get_fields(pcontext->inputschema);
	pcontext->left_in_schema = garrow_schema_new(left_fields);
	pcontext->is_left_schema = true;
	left_source_node = BuildSource(pcontext);
	left_proj_node = BuildJoinProject(outerHashKeys, left_source_node, pcontext);

	/* build right tree */
	pcontext->inputschema = GetSchemaFromSlot((innerPlanState(planstate))->ps_ResultTupleSlot);
	right_fields = garrow_schema_get_fields(pcontext->inputschema);
	pcontext->right_in_schema = garrow_schema_new(right_fields);
	pcontext->is_left_schema = false;
	right_source_node = BuildSource(pcontext);
	right_proj_node = BuildJoinProject(innerHashKeys, right_source_node, pcontext);

	/* build hashjoin tree */
	pcontext->inputschema = NULL;
	pcontext->is_hashjoin_after_node = true;
	if (pcontext->is_nestloopjoin)
	{
		join_node = BuildNestLoopjoin(pcontext, left_proj_node, right_proj_node, joinqual);
		mergeQual = qualList;
	}
	else
	{
		HashJoinState *node = (HashJoinState *) pcontext->planstate;
		if (node->js.jointype == JOIN_INNER)
		{
			join_node = BuildHashjoin(pcontext, left_proj_node, right_proj_node, NULL);
			mergeQual = list_concat(joinqual, qualList);
		}
		else
		{
			join_node = BuildHashjoin(pcontext, left_proj_node, right_proj_node, joinqual);
			mergeQual = qualList;
		}
	}
	pcontext->inputschema = garrow_execute_node_get_output_schema(join_node);

	proj_node = BuildProject(targetList, mergeQual, join_node, pcontext);
	BuildSink(proj_node, estate, pcontext);

	ARROW_FREE(GArrowSchema, &pcontext->left_in_schema);
	ARROW_FREE(GArrowSchema, &pcontext->right_in_schema);
	ARROW_FREE(GArrowSchema, &pcontext->inputschema);
	garrow_list_free_ptr(&left_fields);
	garrow_list_free_ptr(&right_fields);
}

void
FreeVecExecuteState(VecExecuteState *estate)
{
	if (estate->plan)
	{
		glib_autoptr_cleanup_GArrowExecutePlan(&estate->plan);
		estate->plan = NULL;
	}

	if (!estate->pipeline)
	{
		if (estate->reader)
		{
			glib_autoptr_cleanup_GArrowRecordBatchReader(&estate->reader);
			estate->reader = NULL;
		}
	}
	else if (estate->resqueue != NIL)
	{
		list_free(estate->resqueue);
		estate->resqueue = NIL;
	}
}

/* ------------------------------------------------------------------------
 *
 * Execute an Arrow plan to get batch result.
 *
 * ------------------------------------------------------------------------
 */
TupleTableSlot *
ExecuteVecPlan(VecExecuteState *estate)
{
	g_autoptr(GArrowRecordBatch) batch = NULL;
	g_autoptr(GError) error = NULL;

	if (estate->pipeline)
	{
		while (true)
		{
			if (list_length(estate->resqueue) > 0)
			{
				batch = (GArrowRecordBatch *)linitial(estate->resqueue);
				estate->resqueue = list_delete_first(estate->resqueue);
				return ExecStoreBatch(estate->slot, batch);
			}

			if (!estate->started)
			{
				bool isok;
				isok = garrow_execute_plan_pipeline_start(estate->plan, &error);
				if (!isok || error)
					elog(ERROR, "Start execution plan error: %s.", error->message);
				estate->started = true;
			}
			else
			{
				if (garrow_execute_plan_is_stop(estate->plan))
					return NULL;
				garrow_execute_plan_pipeline_continue(estate->plan, &error);
				if (error)
					elog(ERROR, "Continue pipeline execution plan error: %s.", error->message);
			}
		}
	}
	else
	{
		if (!estate->started)
		{
			bool isok;

			isok = garrow_execute_plan_start(estate->plan, &error);
			if (!isok || error)
				elog(ERROR, "Execute plan for vector sort error: %s.", error->message);
			garrow_execute_plan_wait(estate->plan);
			estate->started = true;
		}

		batch = garrow_record_batch_reader_read_next(estate->reader, &error);
		if (error)
			elog(ERROR, "Execute plan for garrow_record_batch_reader_read_next error: %s.", error->message);

		return ExecStoreBatch(estate->slot, batch);
	}
}

/* build sort keys
 *
 * Outputs:
 *   GList of GArrowSortKey.
 */
GList *
build_sort_keys(PlanState *planstate, GArrowSchema *schema, PlanBuildContext *pcontext)
{
	Assert(IsA(planstate->plan, Sort));
	SortSupport sortKeys;
	Sort *plannode = (Sort *)planstate->plan;
	int nkeys = plannode->numCols;
	GList *keys = NULL;
	PlanState  *outerNode = outerPlanState(planstate);
	if (IsA(outerNode, SequenceState))
		outerNode = get_sequence_exact_result_ps(outerNode);
	sortKeys = (SortSupport) palloc0(nkeys * sizeof(SortSupportData));
	for (int i = 0; i < nkeys; i++)
	{
		SortSupport sortKey = sortKeys + i;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = plannode->collations[i];
		sortKey->ssup_nulls_first = plannode->nullsFirst[i];
		sortKey->ssup_attno = plannode->sortColIdx[i];
		/* Convey if abbreviation optimization is applicable in principle */
		sortKey->abbreviate = (i == 0);

		PrepareSortSupportFromOrderingOp(plannode->sortOperators[i], sortKey);
	}
	keys = create_sort_keys(sortKeys, nkeys, schema);
	pfree(sortKeys);
	return keys;
}

/*
 * Should be called by arrow plan, the function is a producer of
 * #GArrowRecordBatch that be consumed by PG plan.
 */
static void
consumer(void *tc, void *rb)
{
	VecExecuteState *estate = (VecExecuteState *)tc;
	GArrowRecordBatch *result = GARROW_RECORD_BATCH(rb);

	estate->resqueue = lappend(estate->resqueue, result);
}

void static
BuildSink(GArrowExecuteNode *input, VecExecuteState *estate, PlanBuildContext *pcontext)
{
	Plan *plan = pcontext->planstate->plan;
	g_autoptr(GArrowSchema) schema = NULL;
	g_autoptr(GArrowExecuteNode) sink = NULL;
	g_autoptr(GArrowSinkNodeOptions) options = NULL;
	g_autoptr(GArrowRecordBatchReader) sinkreader = NULL;
	g_autoptr(GError) error = NULL;

	schema = garrow_execute_node_get_output_schema(input);

	switch(pcontext->sinktype)
	{
		case Selectk:
		{
			GList *keys = NULL;
			g_autoptr(GArrowSelectKOptions) selectkoption = NULL;

			keys = build_sort_keys(pcontext->planstate, schema, pcontext);
			selectkoption = garrow_selectk_options_new(pcontext->nlimit, keys);
			options = GARROW_SINK_NODE_OPTIONS(
					garrow_selectk_sink_node_options_new(selectkoption));
			sink = garrow_execute_plan_build_selectk_sink_node(pcontext->plan, input, GARROW_SELECTK_SINK_NODE_OPTIONS(options), &error);
			garrow_list_free_ptr(&keys);
		}
		break;
		case Orderby:
		{
			GList *keys = NULL;
			g_autoptr(GArrowSortOptions) sortoption = NULL;

			/* Fixme: move ExecSort() to here. */
			keys = build_sort_keys(pcontext->planstate, schema, pcontext);
			sortoption = garrow_sort_options_new(keys);
			options = GARROW_SINK_NODE_OPTIONS(
					garrow_orderby_sink_node_options_new(sortoption));
			sink = garrow_execute_plan_build_orderby_sink_node(pcontext->plan, input,
								 GARROW_ORDERBY_SINK_NODE_OPTIONS(options), &error);
			garrow_list_free_ptr(&keys);

		}
		break;
		case Plain:
		{
			options = garrow_sink_node_options_new();
			sink = garrow_execute_plan_build_sink_node(pcontext->plan,
																 input,
																 options,
																 &error);

		}
		break;
		case Consuming:
		{
			g_autoptr(GArrowSchema) output_schema = NULL;
			g_autoptr(GArrowConsumingSinkNodeOptions) coptions = NULL;

			Assert(pcontext->pipeline);
			output_schema = garrow_execute_node_get_output_schema(input);
			coptions = garrow_consuming_sink_node_options_new(output_schema,
															  consumer,
															  estate);
			sink = garrow_execute_plan_build_consuming_sink_node(pcontext->plan,
																 input,
																 coptions,
																 &error);
		}
		break;

		default:
			elog(ERROR, "Build arrow plan from (%d) type is not support yet.",
					nodeTag(plan));
	}

	if (!pcontext->pipeline)
	{
		sinkreader = garrow_sink_node_options_get_reader(
						GARROW_SINK_NODE_OPTIONS(options), schema);
		garrow_store_ptr(pcontext->reader, sinkreader);
	}

	if (error)
		elog(ERROR, "Failed to create sink node, cause: %s", error->message);
}

// is not distinct equals to  (a == b) or (a is null and b is null)
// is distinct equals to (a != b) or (b is null and a is not null) or (a is null and  b is not null)
// converts to coalesce(a != b, (b is null and a is not null) or (a is null and  b is not null))
static GArrowExpression *
build_is_distinct_expression(DistinctExpr *dex, PlanBuildContext *pcontext)
{
	GList *arguments = NULL;
	g_autoptr(GArrowExpression) a_expr = NULL;
	g_autoptr(GArrowExpression) b_expr = NULL;
	g_autoptr(GArrowExpression) is_distinct = NULL;

	Assert(list_length(dex->args) == 2);

	Expr *a = (Expr *) linitial(dex->args);
	Expr *b = (Expr *) lsecond(dex->args);
	a_expr = expr_to_arrow_expression(a, pcontext);
	b_expr = expr_to_arrow_expression(b, pcontext);

	is_distinct = garrow_is_distinct_expression_new(a_expr, b_expr);

	garrow_list_free_ptr(&arguments);
	return garrow_move_ptr(is_distinct);
}

static GArrowExpression *
build_null_if_expression(NullIfExpr *dex, PlanBuildContext *pcontext)
{
	// nullif can be regarded as a special casewhen expression, 
	// so casewhen is used here for processing
	GList *when_arguments = NULL;
	g_autoptr(GArrowExpression) expr = NULL;
	g_autoptr(GArrowExpression) struct_expr;
	g_autoptr(GArrowExpression) null_expr;
	g_autoptr(GArrowMakeStructOptions) options;
	GList *expr_arguments = NULL;
	gchar **fields;

	Assert(list_length(dex->args) == 2);
	expr = func_args_to_expression(dex->args, pcontext, "equal");
	when_arguments = garrow_list_append_ptr(when_arguments, expr);

	fields = palloc(1 * (sizeof(gchar *)));
	fields[0] = palloc(sizeof(int) + 6);
	snprintf(fields[0], sizeof(int) + 6, "case_%d", 0);
	options = garrow_make_struct_options_new((const gchar **) fields, 1);

	struct_expr = GARROW_EXPRESSION(garrow_call_expression_new(
			"make_struct", when_arguments, GARROW_FUNCTION_OPTIONS(options)));
	expr_arguments = garrow_list_prepend_ptr(expr_arguments, struct_expr);
	
	null_expr = build_literal_expression(GARROW_TYPE_NA, 0, true, exprType((Node *) linitial(dex->args)));
	expr_arguments = garrow_list_append_ptr(expr_arguments, null_expr);
	
	// Use the first arg as the return value
	expr = expr_to_arrow_expression((Expr *) linitial(dex->args), pcontext);
	expr_arguments = garrow_list_append_ptr(expr_arguments, expr);
	
	expr = GARROW_EXPRESSION(garrow_call_expression_new("case_when", expr_arguments, NULL));

	pfree(fields);
	garrow_list_free_ptr(&when_arguments);
	garrow_list_free_ptr(&expr_arguments);
	return garrow_move_ptr(expr);
}


static GArrowExpression *build_filter_expression(List *filterInfo,
												 PlanBuildContext *pcontext)
{
	g_autoptr(GArrowExpression) expr = NULL;

	if (list_length(filterInfo) > 1)
		expr = func_args_to_expression(filterInfo, pcontext, "and_kleene");
	else
		expr = expr_to_arrow_expression((Expr *)linitial(filterInfo), pcontext);
	return garrow_move_ptr(expr);
}

static GArrowFilterNodeOptions *build_filter_options(List *filterInfo,
													 PlanBuildContext *pcontext)
{
	g_autoptr(GArrowFilterNodeOptions) filter_options = NULL;
	g_autoptr(GArrowExpression) expr = build_filter_expression(filterInfo, pcontext);
	filter_options = garrow_filter_node_options_new(expr);
	return garrow_move_ptr(filter_options);
}

static void *
get_scan_next_batch(PlanState *node)
{
	VecSeqScanState *vnode = (VecSeqScanState *)node;
	TupleTableSlot *slot;

	while (true)
	{
		slot = ExecVecScanFetch((ScanState *)node,
								(ExecScanAccessMtd)VecSeqNext,
								(ExecScanRecheckMtd)VecSeqRecheck,
								vnode->vscanslot);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));

	return (void *) GetBatch(slot);
}

static void *
get_foreign_next_batch(PlanState *node)
{
	VecForeignScanState *vnode = (VecForeignScanState *)node;
	TupleTableSlot *slot;

	while (true)
	{
		slot = ExecVecScanFetch((ScanState *)node,
								(ExecScanAccessMtd)ForeignNext,
								(ExecScanRecheckMtd)ForeignRecheck,
								vnode->vscanslot);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));

	return (void *) GetBatch(slot);
}

static void *
get_current_next_batch(PlanState *node)
{
	TupleTableSlot *slot;

	while (true) {
		slot = ExecProcNode(node);
		if (TupIsNull(slot))
			return NULL;
		if (!GetNumRows(slot))
			continue;
		break;
	}

	Assert(!TTS_IS_DIRTY(slot));
	return (void *) GetBatch(slot);
}

static GArrowExecuteNode *
BuildSource(PlanBuildContext *pcontext)
{
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowRecordBatchReader)	reader = NULL;
	g_autoptr(GArrowSourceNodeOptions)	options = NULL;
	g_autoptr(GArrowExecuteNode) source = NULL;

	PlanState *pstate = pcontext->planstate;
	GetNextCallback callback;
	switch(nodeTag(pcontext->planstate))
	{
		case T_SeqScanState:
			callback = (GetNextCallback) get_scan_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_ForeignScanState:
			callback = (GetNextCallback) get_foreign_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_SequenceState:
		case T_AppendState:
		case T_MotionState:
			callback = (GetNextCallback) get_current_next_batch;
			pstate = pcontext->planstate;
			break;
		case T_HashJoinState:
		case T_NestLoopState:
			callback = (GetNextCallback) get_current_next_batch;
			if (pcontext->is_left_schema) 
				pstate = outerPlanState(pcontext->planstate);
			else
				pstate = innerPlanState(pcontext->planstate);
			break;
		case T_SubqueryScanState:
			callback = (GetNextCallback) get_current_next_batch;
			SubqueryScanState *subscanstate = (SubqueryScanState *) pcontext->planstate;
			pstate = subscanstate->subplan;
			break;
		default:
			callback = (GetNextCallback) get_current_next_batch;
			pstate = outerPlanState(pcontext->planstate);
			break;
	}

	reader = garrow_record_batch_reader_new_callback(
		pcontext->inputschema,
		callback,
		pstate,
		&error);

	if (error)
		elog(ERROR, "Failed to create callback reader for source node, cause: %s.", error->message);

	options = garrow_source_node_options_new_record_batch_reader(reader);
	if (!pcontext->pipeline || !pcontext->is_left_schema)
		source = garrow_execute_plan_build_source_node(pcontext->plan,
													   options,
													  &error);
	else
		source = garrow_execute_plan_build_pipeline_source_node(pcontext->plan,
																options,
															   &error);
	if (error)
		elog(ERROR, "Failed to create source node, cause: %s.", error->message);

	return garrow_move_ptr(source);
}

/* build project for aggregate input */
static GArrowExecuteNode*
BuildAggProject(List *targetList, List *aggInfos, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GError) error = NULL;

	if (IsA(pcontext->planstate, WindowAggState))
		project_options = build_winagg_project_options(targetList, aggInfos, pcontext);
	else
		project_options = build_agg_project_options(targetList, aggInfos, pcontext);

	if (!project_options)
		return NULL;

	project = garrow_execute_plan_build_project_node(pcontext->plan,
													 input,
													 project_options,
													 &error);
	if (error)
		elog(ERROR, "Failed to create Arrow project node: %s.", error->message);
	return garrow_move_ptr(project);
}

static void free_agg_infos(List *agginfos)
{
	ListCell	   *l;

	foreach(l, agginfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);

		glib_autoptr_cleanup_GArrowFunctionOptions(&agginfo->options);
	}
	list_free_deep(agginfos);
}

static GArrowExecuteNode*
BuildProject(List *targetList, List *qualList, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowFilterNodeOptions) filter_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GArrowExecuteNode) filter_node = NULL;
	g_autoptr(GArrowExecuteNode) orderby_node = NULL;
	g_autoptr(GArrowExecuteNode) current = garrow_copy_ptr(input);
	g_autoptr(GError) error = NULL;

	if (targetList)
		project_options = build_project_options(targetList, pcontext);
	if (qualList)
		filter_options = build_filter_options(qualList, pcontext);

	/* Build Agg or WindowAgg if any */
	if (IsA(pcontext->planstate->plan, Agg) ||
	    IsA(pcontext->planstate->plan, WindowAgg))
	{
		g_autoptr(GArrowExecuteNode) aggregation_input = NULL;
		g_autoptr(GArrowExecuteNode) aggregation = NULL;
		List *agginfos = pcontext->agginfos;

		if (IsA(pcontext->planstate, AggState) && IsA(pcontext->planstate->lefttree, SortState))
		{
			VecSortState *state = (VecSortState *)outerPlanState(pcontext->planstate);
			if (state->skip)
			{
				orderby_node = build_orderby_node(pcontext->planstate, pcontext->plan, current, pcontext);
			}
		}

		/* build project for aggregate input */
		if (orderby_node)
			aggregation_input = BuildAggProject(targetList, agginfos, orderby_node, pcontext);
		else
			aggregation_input = BuildAggProject(targetList, agginfos, current, pcontext);

		if (aggregation_input)
			aggregation = BuildAggregatation(agginfos, aggregation_input, pcontext); 
		else if (orderby_node)
			aggregation = BuildAggregatation(agginfos, orderby_node, pcontext);
		else
			aggregation = BuildAggregatation(agginfos, current, pcontext);
		garrow_store_ptr(current, aggregation);

		free_agg_infos(agginfos);
	}

	/* having clause is always after agg and before upper agg project */
	if (qualList)
	{
		filter_node = garrow_execute_plan_build_filter_node(pcontext->plan, current, filter_options, &error);
		if (error)
			elog(ERROR, "Failed to create filter node, %s.", error->message);
		garrow_store_ptr(current, filter_node);
	}

	if (targetList)
	{
		project = garrow_execute_plan_build_project_node(pcontext->plan,
														 current,
														 project_options,
														 &error);
		if (error)
			elog(ERROR, "Failed to create Arrow project node: %s.", error->message);
		garrow_store_ptr(current, project);
	}

	return garrow_move_ptr(current);
}

static const char *
GetHashJoinProjectName(PlanBuildContext *pcontext, const char *name)
{
	/* right_ len > left_ len */
	int prefix_maxlen = strlen(RIGHT_PREFIX);
	char *prefix_name = palloc(prefix_maxlen + strlen(name) + 1);
	snprintf(prefix_name, prefix_maxlen + strlen(name) + 1, "%s%s", pcontext->is_left_schema ? LEFT_PREFIX : RIGHT_PREFIX, name);
	return prefix_name;
}

static GArrowProjectNodeOptions *
build_join_project_options(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	ListCell	   *l;
	const gchar		  **names;
	g_autoptr(GArrowSchema) schema = NULL;
	GList *fields = NULL;
	g_autoptr(GError) error = NULL;
	int i = 0;
	int j = 0;

	schema = garrow_execute_node_get_output_schema(input);
	fields = garrow_schema_get_fields(schema);

	length = g_list_length(fields) + list_length(hashkeys);
	names = palloc(length * sizeof(gchar *));

	/* convert source schema column to proj fields */
	for (j = 0; j < g_list_length(fields); j++)
	{
		g_autoptr(GArrowExpression) arrow_expr;
		g_autoptr(GArrowField) f = garrow_list_nth_data(fields, j);
		const char *name = garrow_field_get_name(f);
		arrow_expr = GARROW_EXPRESSION(garrow_field_expression_new(name, &error));
		if (error)
			elog(ERROR, "convert field to arrow expression failed: %s",
				 error->message);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		names[j] = GetHashJoinProjectName(pcontext, name);
	}

	/* append hashkeys column to proj fields */
	foreach(l, hashkeys)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField) field = NULL;
		g_autoptr(GArrowDataType) result_type = NULL; 
		ExprState *exprstate = (ExprState *)lfirst(l);
		int prefix_maxlen = strlen(RIGHT_PREFIX);
		char *prefix_name = palloc(prefix_maxlen + 20);
		/* The hashkey's projected column names are built together when building the expression */
		arrow_expr = expr_to_arrow_expression(exprstate->expr, pcontext);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		snprintf(prefix_name, prefix_maxlen + 20, "%sjoinqual_%d", pcontext->is_left_schema ? LEFT_PREFIX : RIGHT_PREFIX, i++);
		result_type = PGTypeToArrow(exprType((Node *) exprstate->expr));
		field = garrow_field_new(prefix_name, result_type);
		if (pcontext->is_left_schema)
		{
			pcontext->left_hashkeys = garrow_list_append_ptr(pcontext->left_hashkeys, field);
		}
		else
		{
			pcontext->right_hashkeys = garrow_list_append_ptr(pcontext->right_hashkeys, field);
		}
		names[j++] = prefix_name;
	}
	options = garrow_project_node_options_new(expressions,
											  (gchar **)names,
											  length);
	pfree(names);
	garrow_list_free_ptr(&expressions);
	garrow_list_free_ptr(&fields);
	return garrow_move_ptr(options);
}

static GArrowExecuteNode*
BuildJoinProject(List *hashkeys, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowProjectNodeOptions) project_options = NULL;
	g_autoptr(GArrowExecuteNode) project = NULL;
	g_autoptr(GError) error = NULL;

	project_options = build_join_project_options(hashkeys, input, pcontext);

	project = garrow_execute_plan_build_project_node(pcontext->plan,
													 input,
													 project_options,
													 &error);
	if (error)
		elog(ERROR, "Failed to create Arrow project node: %s.", error->message);
	return garrow_move_ptr(project);
}

static const int
GetGroupKey(Agg *agg, Expr *expr)
{
	Var *var = (Var *)expr;

	if (!IsA(expr, Var))
		return -1;

	for(int i = 0; i < agg->numCols; i++)
	{
		if(agg->grpColIdx[i] == var->varattno)
			return i;
	}
	return -1;
}

/* Aggregate input columns is consists of two parts:
 * 1. The arg fo each aggref.
 * 2. The group keys columns if exists.
 * Make these expressions as a single projector.
 */
static GArrowProjectNodeOptions *
build_agg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	Agg *agg = (Agg *) pcontext->planstate->plan;
	gsize nkey = agg->numCols;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	int				i = 0;
	bool 			need_project = false;
	ListCell	   *l;
	const gchar	  **names; /* project result names */

	length = list_length(aggInfos) + nkey;
	names = palloc(length * sizeof(gchar *));
	/* append group keys */
	for (i = 0; i < nkey; i++)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField)	field = NULL;
		g_autoptr(GError) error = NULL;

		/* group key out name is same as input name */
		names[i] = GetSchemaName(pcontext->inputschema, agg->grpColIdx[i], pcontext->map);
		arrow_expr = GARROW_EXPRESSION(
				garrow_field_expression_new(names[i], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
					names[i], error->message);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
	}
	if (nkey > 0)
		pcontext->keys = names;
	pcontext->nkey = nkey;

	if ((pcontext->nkey > 0) && (!aggInfos))
		rewrite_tl_keys(targetList, pcontext);

	/* append aggrefs */
	foreach(l, aggInfos)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		Aggref *aggref = agginfo->aggref;
		int key;
		TargetEntry *source_tle;

		Assert(IsA(aggref, Aggref));
		/* Todo: single arg only now*/

		if(!aggref->args)
			continue;

		if (list_length(aggref->args) != 1) 
			elog(ERROR, "doesn't support aggref args len != 1");
		source_tle = (TargetEntry *) linitial(aggref->args);

		/* skip columns in group keys*/
		key = GetGroupKey(agg, source_tle->expr);
		if(key >= 0)
		{
			/* aggref is group key, update agg inname */
			memcpy(agginfo->inname, names[key], sizeof(agginfo->inname));
			continue;
		}

		arrow_expr = expr_to_arrow_expression(source_tle->expr, pcontext);

		/* FIXME: Add shared input expression later */

		/* child expression is input of agg, use inname */
		names[i] = agginfo->inname;

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		need_project = true;
		i++;
	}

	/* append aggrefs */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		Aggref *aggref = agginfo->aggref;

		/* count() or count(*), needn't project this column.
		 * Update aggname as another valid column.
		 */
		if(!aggref->args)
		{
			ListCell	   *c;
			char *name = NULL;

			foreach(c, aggInfos)
			{
				VecAggInfo *cinfo = (VecAggInfo *) lfirst(c);

				if (cinfo->aggref->args)
				{
					name = cinfo->inname;
					break;
				}
			}

			/* If found other agg, count other column.
			 * If no other column, means a count(*), no project for agg input.
			 * So, use an arbitrary input column name as agg inname.
			 */
			/* Fixme: implement count(*) by arrow EmptyBatch */
			if (!name)
			{
				name = (char *)GetSchemaName(pcontext->inputschema, 1, pcontext->map);
			}
			memcpy(agginfo->inname, name, sizeof(agginfo->inname));
			continue;
		}
	}

	if (!need_project)
	{
		garrow_list_free_ptr(&expressions);
		return NULL;
	}

	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  i);

	/* if nkeys > 0, names will be used for group keys */
	if (nkey <= 0)
		pfree(names);
	garrow_list_free_ptr(&expressions);
	return garrow_move_ptr(options);
}

static GArrowProjectNodeOptions *
build_winagg_project_options(List *targetList, List *aggInfos, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	bool 			need_project = false;
	ListCell	   *l;
	const gchar	  **names; /* project result names */

	WindowAgg *wagg = (WindowAgg *) pcontext->planstate->plan;

	/* find cols used by "order by" clause */
	pcontext->orderby_sortoption = NULL;
	if (wagg->ordNumCols > 0)
	{
		GList *orderby_keys = NULL;
		SortKey sortKey;
		sortKey.orders = palloc0(sizeof(GArrowSortOrder) * wagg->ordNumCols);
		sortKey.nulls_first = palloc0(sizeof(GArrowSortNullPlacement) * wagg->ordNumCols);
		get_windowagg_sortorder(pcontext, &sortKey);

		for (int i = 0; i < wagg->ordNumCols; i++)
		{
			g_autoptr(GArrowSortKey) key = NULL;
			const char *field_name = GetSchemaName(pcontext->inputschema, wagg->ordColIdx[i], NULL);
			key = garrow_sort_key_new((gchar *)pstrdup(field_name), sortKey.orders[i], GARROW_SORT_ORDER_Default, sortKey.nulls_first[i]);
			orderby_keys = garrow_list_append_ptr(orderby_keys, key);
		}

		pcontext->orderby_sortoption = garrow_sort_options_new(orderby_keys);
		garrow_list_free_ptr(&orderby_keys);
		pfree(sortKey.orders);
		pfree(sortKey.nulls_first);
	}

	/* find cols used by "partition by" clause */
	const gchar **partcol_names = palloc0(sizeof(gchar *) * wagg->partNumCols);
	for (int i = 0; i < wagg->partNumCols; i++)
		partcol_names[i] = GetSchemaName(pcontext->inputschema, wagg->partColIdx[i], NULL);

	if (wagg->partNumCols > 0)
	{
		pcontext->keys = partcol_names;
		pcontext->nkey = wagg->partNumCols;
	}
	else
	{
		pcontext->keys = NULL;
		pcontext->nkey = 0;
	}

	/* append all input columns with windowagg */
	int num_fields = garrow_schema_n_fields(pcontext->inputschema);
	length = list_length(aggInfos) + num_fields;
	names = palloc(length * sizeof(gchar *));
	int col = 0;
	for (int i = 0; i < num_fields; i++)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		g_autoptr(GArrowField)	field = NULL;
		g_autoptr(GError) error = NULL;

		/* group key out name is same as input name */
		const char *fname = GetSchemaName(pcontext->inputschema, i + 1, pcontext->map);
		names[col] = fname;
		arrow_expr = GARROW_EXPRESSION(garrow_field_expression_new(names[col], &error));
		if (error)
			elog(ERROR, "New arrow expression from column: %s error: %s",
					names[i], error->message);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		++col;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		Assert(IsA(wfunc, WindowFunc));

		/* Todo: single arg only now*/
		if(!wfunc->args)
			continue;

		Assert(list_length(wfunc->args) == 1);
		arrow_expr = expr_to_arrow_expression(linitial(wfunc->args), pcontext);

		/* FIXME: Add shared input expression later */

		/* child expression is input of agg, use inname */
		names[col] = agginfo->inname;

		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		need_project = true;
		col++;
	}

	/* append window aggrefs */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);
		WindowFunc *wfunc = agginfo->wfunc;

		/*
		 * count() or count(*), needn't project this column.
		 * Update aggname as another valid column.
		 */
		if(!wfunc->args)
		{
			ListCell	   *c;
			char *name = NULL;

			foreach(c, aggInfos)
			{
				VecAggInfo *cinfo = (VecAggInfo *) lfirst(c);

				if (cinfo->wfunc->args)
				{
					name = cinfo->inname;
					break;
				}
			}

			/* If found other agg, count other column.
			 * If no other column, means a count(*), no project for agg input.
			 * So, use an arbitrary input column name as agg inname.
			 */
			/* Fixme: implement count(*) by arrow EmptyBatch */
			if (!name)
			{
				name = (char *)GetSchemaName(pcontext->inputschema, 1, pcontext->map);
			}
			memcpy(agginfo->inname, name, sizeof(agginfo->inname));
			continue;
		}
	}

	if (!need_project)
	{
		garrow_list_free_ptr(&expressions);
		return NULL;
	}

	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  col);

	if (DEBUG1 >= log_min_messages)
	{
		for (GList *node = expressions; node; node = node->next)
		{
			g_autofree gchar *str =
				garrow_expression_to_string(GARROW_EXPRESSION(node->data));
			elog(DEBUG1, "%s result expressions: %s", __func__, str);
		}
	}
	garrow_list_free_ptr(&expressions);

	return garrow_move_ptr(options);
}

static GArrowProjectNodeOptions *
build_project_options(List *targetList, PlanBuildContext *pcontext)
{
	GList *expressions = NULL;
	g_autoptr(GArrowProjectNodeOptions) options = NULL;
	gsize			length;
	int				i = 0;
	ListCell	   *l;
	const gchar		  **names;

	length = list_length(targetList);
	names = palloc(length * sizeof(gchar *));

	foreach(l, targetList)
	{
		g_autoptr(GArrowExpression) arrow_expr = NULL;

		TargetEntry *tle = (TargetEntry *)lfirst(l);
		/* rebuild Target Entry to List*/
		Assert(IsA(tle, TargetEntry));
		/* use targetEntry name */
		arrow_expr = expr_to_arrow_expression(tle->expr, pcontext);
		names[i] = (gchar *)GetUniqueAttrName(tle->resname, i + 1);
		expressions = garrow_list_append_ptr(expressions, arrow_expr);
		i++;
	}
	options = garrow_project_node_options_new(expressions,
											  (gchar**)names,
											  length);
	pfree(names);

	if (DEBUG1 >= log_min_messages)
	{
		for (GList *node = expressions; node; node = node->next)
		{
			g_autofree gchar *str =
				garrow_expression_to_string(GARROW_EXPRESSION(node->data));
			elog(DEBUG1, "%s result expressions: %s", __func__, str);
		}
	}
	garrow_list_free_ptr(&expressions);

	return garrow_move_ptr(options);
}

/*
 * The distinct of group by key needs to carry other fields, which is the default for pg, 
 * but it needs to be provided for arrow, otherwise the column will not be found when build _outagg_dummy agg.
 * 
 * case1: fallback
 * explain verbose SELECT DISTINCT count(unique1) FROM tenk1 group by unique1;
 * ->  HashAggregate  (cost=183.33..216.67 rows=3333 width=12)
 *       Output: (count(unique1)), unique1
 *       Group Key: (count(tenk1.unique1))
 *
 * case2: normal
 * explain verbose select unique1 from tenk1 a where unique1 in
 * (select unique1 from tenk1 b join tenk1 c using (unique1) where b.unique2 = 42);
 * ->  HashAggregate  
 *       Output: b.unique1, c.unique1
 *       Group Key: b.unique1
 * 
 * for case1, we should fallback since unique1 is junk column, we will get n rows, but acutal one result.
 * for case2, normal process.
 */
static void 
rewrite_tl_keys(List *targetList, PlanBuildContext *pcontext)
{
	Agg *agg = (Agg *) pcontext->planstate->plan;
	gsize nkey = agg->numCols;
	int length = list_length(targetList);
	const gchar	  **keys;
	int j;
	ListCell	   *l;

	/*
	 * targetList may be empty.
	 * Vec GroupAggregate  (cost=0.00..431.00 rows=1 width=1)
     * 	   Group Key: col_a
	 */
	if (!targetList)
		return;

	j = 0;
	keys = palloc(length * sizeof(gchar *));
	foreach(l, targetList)  
	{
		TargetEntry *tle = lfirst(l);
		Node *node = (Node *)tle->expr;
		bool find = false;
		const char* cur;
		if (nodeTag(node) != T_Var)
			continue;
		Var *var = (Var *)node;
		int i = 0;
		for (i = 0; i < nkey; i++) 
		{
			AttrNumber attnum = agg->grpColIdx[i];
			if (var->varattno == attnum)
				break;
		}
		if (i >= nkey) /* not found */
			cur = GetSchemaName(pcontext->inputschema, var->varattno, pcontext->map);
		else
			cur = pcontext->keys[i];
		/*
		 * SELECT a/2, a/2 FROM test_missing_target group by a/2;
		 * ->  Vec GroupAggregate  (cost=0.00..431.00 rows=1 width=4)
         * Output: ((a / 2)), ((a / 2))
         * Group Key: ((test_missing_target.a / 2))
		 */
		/* targetList has the same key */
		for (int k = 0; k < j; k++)
		{
			if (strcmp(keys[k], cur) == 0)
			{
				find = true;
				break;
			}
		}
		if (!find)
			keys[j++] = cur;
	}
	pcontext->keys = keys;	
	pcontext->nkey = j;	
}


static GArrowExecuteNode*
build_orderby_node(PlanState *planstate, GArrowExecutePlan *plan,  GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	int nkeys;
	GList *sort_keys = NULL;
	g_autoptr(GError) error = NULL;
	g_autoptr(GArrowSortOptions) sortoption = NULL;
	g_autoptr(GArrowOrderbyNodeOptions) orderby_options = NULL;
	GArrowExecuteNode *orderby_node = NULL;
	g_autoptr(GArrowSchema) schema = NULL;

	VecSortState *node = (VecSortState *)outerPlanState(planstate);
	Sort	   *plannode = (Sort *) node->base.ss.ps.plan;

	schema = garrow_execute_node_get_output_schema(input);
	nkeys = plannode->numCols;

	/* sort option */
	sort_keys = build_sort_keys((PlanState *)node, schema, pcontext);
	sortoption = garrow_sort_options_new(sort_keys);
	orderby_options = garrow_orderby_node_options_new(sortoption);
	orderby_node = garrow_execute_plan_build_orderby_node(plan, input, orderby_options, &error);
	if (error)
		elog(ERROR, "Failed to create orderby node, cause: %s", error->message);
	garrow_list_free_ptr(&sort_keys);
	return orderby_node;
}

static GArrowExecuteNode *
BuildAggregatation(List *aggInfos, GArrowExecuteNode *input, PlanBuildContext *pcontext)
{
	ListCell	   *l;
	GList *aggregations = NULL;
	g_autoptr(GArrowExecuteNode) aggregate = NULL;
	g_autoptr(GArrowExecuteNode) orderbynode = NULL;
	g_autoptr(GArrowAggregation) agg_func = NULL;
	g_autoptr(GArrowAggregateNodeOptions) options = NULL;
	g_autoptr(GError) error = NULL;

	/* build aggregations */
	foreach(l, aggInfos)
	{
		VecAggInfo *agginfo = (VecAggInfo *) lfirst(l);

		/*Fixme only normal agg now. */
		if (true)
		{
			agg_func = garrow_aggregation_new(
					   agginfo->aname,
					   agginfo->options,
					   agginfo->inname,
					   agginfo->outname);
			aggregations = garrow_list_append_ptr(aggregations, agg_func);
		}
	}

	/* no aggref in targetlist, append plain/hash distinct for group by columns.
	 * Out name will not be used, use dummy.
	 */
	if (!IsA(pcontext->planstate, WindowAggState) && ((pcontext->nkey > 0) && (!aggregations)))
	{
		agg_func = garrow_aggregation_new(
			pcontext->aggstrategy == AGG_SORTED ? "plain_distinct" : "hash_distinct",
			NULL,
			pcontext->keys[0],
			"_outagg_dummy");
		aggregations = garrow_list_append_ptr(aggregations, agg_func);
	}

	options = build_aggregatation_options(aggregations, pcontext);

	aggregate = garrow_execute_plan_build_aggregate_node(pcontext->plan,
														input,
														options,
														&error);
	if (error)
		elog(ERROR, "Failed to create agg node, cause: %s", error->message);

	/* release agginfos to prevent building aggregations repeatedly*/
	garrow_list_free_ptr(&aggregations);
	return garrow_move_ptr(aggregate);
}

static GArrowAggregateNodeOptions *
build_aggregatation_options(GList *aggregations, PlanBuildContext *pcontext)
{
	g_autoptr(GArrowAggregateNodeOptions) options = NULL;
	g_autoptr(GError) error = NULL;

	if (IsA(pcontext->planstate, WindowAggState))
	{
		int mode = garrow_aggregate_get_window_mode();
		options =
			garrow_general_aggregate_node_options_new(aggregations,
													pcontext->keys,
													pcontext->nkey,
													mode,
													pcontext->orderby_sortoption,
													&error);
	}
	else if (pcontext->aggstrategy == AGG_SORTED)
		options = garrow_ordered_aggregate_node_options_new(aggregations,
															pcontext->keys,
															pcontext->nkey,
															&error);
	else
		options = garrow_aggregate_node_options_new(aggregations,
													pcontext->keys,
													pcontext->nkey,
													&error);
	if (error)
		elog(ERROR, "Failed to create agg node options, cause: %s", error->message);

	if (pcontext->keys)
		pfree(pcontext->keys);

	return garrow_move_ptr(options);
}

static GArrowJoinType
to_arrow_jointype(JoinType type, List *joinqual)
{
	switch (type)
	{
		case JOIN_INNER:
			return GARROW_INNER_JOIN;
		case JOIN_LEFT:
			return GARROW_LEFT_OUTER_JOIN;
		case JOIN_RIGHT:
			return GARROW_RIGHT_OUTER_JOIN;
		case JOIN_FULL:
			return GARROW_FULL_OUTER_JOIN;
		case JOIN_SEMI:
			return GARROW_FULL_SEMI_JOIN;
		case JOIN_ANTI:
			return GARROW_FULL_ANTI_JOIN;
		case JOIN_LASJ_NOTIN:
			return GARROW_LASJ_NOTIN_JOIN;
		default:
			elog(ERROR, "join type not supported by arrow join %d", type);
	}

	/* never run here */
	return GARROW_INNER_JOIN;
}

static GArrowExecuteNode *
BuildHashjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual)
{
	GError *error = NULL;
	HashJoinState *node;
	VecHashJoinState *vnode;
	GArrowJoinType type;
	GArrowExecuteNode *hashjoin_node;
	g_autolist(GObject) lkeys = NULL;
	g_autolist(GObject) rkeys = NULL;
	g_autoptr(GArrowSchema) left_schema = NULL;
	g_autoptr(GArrowSchema) right_schema = NULL;
	g_autoptr(GArrowHashJoinNodeOptions) hashjoin_options = NULL;
	g_autoptr(GArrowExpression) filter_expr = NULL;

	node = (HashJoinState *) pcontext->planstate;
	vnode = (VecHashJoinState *) node;
	left_schema = garrow_execute_node_get_output_schema(left);
	right_schema = garrow_execute_node_get_output_schema(right);
	type  = to_arrow_jointype(node->js.jointype, joinqual);
	lkeys = garrow_schema_get_fields(left_schema);
	rkeys = garrow_schema_get_fields(right_schema);
	pcontext->left_proj_schema = left_schema;
	pcontext->right_proj_schema = right_schema;
	if (joinqual)
		filter_expr = build_filter_expression(joinqual, pcontext);
	if (vnode->joinqual_pushdown)
	{
		GArrowField *left_field = garrow_schema_get_field(pcontext->left_in_schema, vnode->left_attr_in_joinqual);
		const gchar *left_field_name = garrow_field_get_name(left_field);

		GArrowField *right_field = garrow_schema_get_field(pcontext->right_in_schema, vnode->right_attr_in_joinqual);
		const gchar *right_field_name = garrow_field_get_name(right_field);

		const int nkey = 3;
		gchar **filter_names = palloc(nkey * sizeof(gchar *));
		int prefix_maxlen = strlen(RIGHT_PREFIX);
		char *left_name = palloc(prefix_maxlen + strlen(left_field_name) + 1);
		char *right_name = palloc(prefix_maxlen + strlen(right_field_name) + 1);
		char* not_equal = pstrdup("not_equal");
		snprintf(left_name, NAMEDATALEN, "%s%s", LEFT_PREFIX, left_field_name);
		snprintf(right_name, NAMEDATALEN, "%s%s", RIGHT_PREFIX, right_field_name);
		filter_names[0] = left_name;
		filter_names[1] = right_name;
		filter_names[2] = not_equal;

		hashjoin_options =
			garrow_hash_join_node_options_new(type, pcontext->left_hashkeys, pcontext->right_hashkeys, NULL, filter_names, nkey, filter_expr,
											  "", "", false, &error);
		pfree(left_name);
		pfree(right_name);
		pfree(not_equal);
		pfree(filter_names);
		if (error)
			elog(ERROR, "Failed to create the hashjoin node, cause: %s", error->message);
	}
	else
	{
		GList *keycmps = NULL;
		if (node->hj_nonequijoin)
		{
			for (int i = 0; i < g_list_length(lkeys); ++i)
			{
				gpointer cmp = (gpointer) GARROW_JOIN_CMP_IS;
				keycmps = garrow_list_append_ptr(keycmps, cmp);
			}
		}
		hashjoin_options =
			garrow_hash_join_node_options_new(type, pcontext->left_hashkeys, pcontext->right_hashkeys, keycmps, NULL, 0, filter_expr,
											  "", "", false, &error);
		if (error)
			elog(ERROR, "Failed to create the hashjoin node, cause: %s", error->message);
	}

	hashjoin_node =
		garrow_execute_plan_build_hash_join_node(pcontext->plan, left, right,
												 hashjoin_options, &error);
	if (error)
		elog(ERROR, "Failed to create the hashjoin node, cause: %s", error->message);

	garrow_list_free_ptr(&pcontext->left_hashkeys);
	garrow_list_free_ptr(&pcontext->right_hashkeys);
	return hashjoin_node;
}

static bool *
get_sortorder_by_opoid(int num, Oid *ops)
{
	bool *order_reverse = palloc0(sizeof(bool) * num);
	for (int i = 0; i < num; ++i)
	{
		Oid			opfamily;
		Oid			opcintype;
		int16		strategy;

		/* Find the operator in pg_amop */
		if (!get_ordering_op_properties(ops[i], &opfamily, &opcintype, &strategy))
			elog(ERROR, "operator %u is not a valid ordering operator", ops[i]);
		order_reverse[i] = (strategy == BTGreaterStrategyNumber);
	}

	return order_reverse;
}

static bool *
create_order_reverse(Plan *plan, int *numCols, AttrNumber **sortColIdx)
{
	bool *order_reverse = NULL;

	if (IsA(plan, Sort))
	{
		Sort *sort   = (Sort *)plan;
		*numCols     = sort->numCols;
		*sortColIdx  = sort->sortColIdx;
		order_reverse = get_sortorder_by_opoid(sort->numCols,
		                                       sort->sortOperators);
	}
	else if (IsA(plan, Motion))
	{
		Motion *motion = (Motion *)plan;
		*numCols       = motion->numSortCols;
		*sortColIdx    = motion->sortColIdx;
		order_reverse = get_sortorder_by_opoid(motion->numSortCols,
		                                       motion->sortOperators);
	}
	else
	{
		Assert(0); /* never run here */
		const char *nodestr = nodeTypeToString(plan);
		elog(ERROR, "invalid child node type: %s under WindowAgg.", nodestr);
	}

	return order_reverse;
}

static bool *
get_nulls_first(Plan *plan) 
{

	if (IsA(plan, Sort))
	{
		Sort *sort   = (Sort *)plan;
		return sort->nullsFirst;
	}
	else if (IsA(plan, Motion))
	{
		Motion *motion = (Motion *)plan;
		return motion->nullsFirst;
	}
	return NULL;
}

static AttrNumber
find_input_attno_in_targetlist(Plan *plan, AttrNumber attno)
{
	if (attno > list_length(plan->targetlist))
		return -1;

	TargetEntry *tle = (TargetEntry *)list_nth(plan->targetlist, attno - 1);
	if (!IsA(tle->expr, Var))
		return -1;

	Var *var = (Var *)tle->expr;
	if (var->varattno == 0)
		return -1;

	return var->varattno;
}

static void
get_windowagg_sortorder(PlanBuildContext *pcontext, SortKey *sortKey)
{
	int numCols              = 0;
	AttrNumber *sortColIdx   = NULL;
	bool *order_reverse      = NULL;
	bool *nulls_first 		 = NULL;

	Assert(IsA(pcontext->planstate->plan, WindowAgg));
	WindowAgg *wagg = (WindowAgg *) pcontext->planstate->plan;

	for (int i = 0; i < wagg->ordNumCols; ++i)
	{
		/*
		 * for every sort column in wagg, do:
		 * 1. find attno in the input slot of wagg;
		 * 2. find attno in the input slot of sort/motion with attno in wagg by
		 *    the targetlist of sort/motion;
		 */
		AttrNumber attno = wagg->ordColIdx[i];

		Plan *child = (Plan *)outerPlan(wagg);
		while (child)
		{
			AttrNumber input_attno = -1;
			input_attno = find_input_attno_in_targetlist(child, attno);
			if (input_attno == -1)
				elog(ERROR, "order column: %d in WindowAgg NOT found"
				            " in the lower node", wagg->ordColIdx[i]);

			attno = input_attno;

			if ((IsA(child, Agg) && ((Agg *)child)->aggstrategy == AGG_SORTED) ||
				IsA(child, WindowAgg))
				child = outerPlan(child);
			else
				break;
		}

		if (!nulls_first)
			nulls_first = get_nulls_first(child);

		if (!order_reverse)
			order_reverse = create_order_reverse(child, &numCols, &sortColIdx);

		/*
		 * find the position of the sort order by the attno
		 */
		int nth = 0;
		for (; nth < numCols; ++nth)
		{
			if (sortColIdx[nth] == attno)
				break;
		}
		if (nth == numCols)
			elog(ERROR, "order column: %d in WindowAgg NOT found"
			            " in the lower node", wagg->ordColIdx[i]);

		if (order_reverse[nth])
			sortKey->orders[i] = GARROW_SORT_ORDER_DESCENDING;
		else
			sortKey->orders[i] = GARROW_SORT_ORDER_ASCENDING;
		
		if (nulls_first && nulls_first[i])
			sortKey->nulls_first[i] = GARROW_SORT_ORDER_AT_START;
		else
			sortKey->nulls_first[i] = GARROW_SORT_ORDER_AT_END;
	}
}

const char *
nodeTypeToString(const void *obj)
{
	char *nodestr = nodeToString(obj);
	nodestr++;
	char *end = strchr(nodestr, ' ');
	if (end)
		*end = '\0';

	return nodestr;
}
static GArrowExecuteNode *
BuildNestLoopjoin(PlanBuildContext *pcontext, GArrowExecuteNode *left, GArrowExecuteNode *right, List *joinqual)
{
	GError *error = NULL;
	NestLoopState *node;
	GArrowJoinType type;
	GArrowExecuteNode *nestedloopjoin_node;
	g_autoptr(GArrowSchema) left_schema = NULL;
	g_autoptr(GArrowSchema) right_schema = NULL;
	g_autoptr(GArrowNestedLoopJoinNodeOptions) nest_loop_options = NULL;
	g_autoptr(GArrowExpression) filter_expr = NULL;

	node = (NestLoopState *) pcontext->planstate;
	left_schema = garrow_execute_node_get_output_schema(left);
	right_schema = garrow_execute_node_get_output_schema(right);
	pcontext->left_proj_schema = left_schema;
	pcontext->right_proj_schema = right_schema;
	type  = to_arrow_jointype(node->js.jointype, joinqual);
	if (joinqual)
		filter_expr = build_filter_expression(joinqual, pcontext);
	else 
		filter_expr = build_literal_expression(GARROW_TYPE_BOOLEAN, true, false, InvalidOid);
	nest_loop_options =
		garrow_nested_loop_join_node_options_new(type, filter_expr, "", "", &error);
	if (error)
		elog(ERROR, "Failed to create the nestedloopjoin node, cause: %s", error->message);
	nestedloopjoin_node =
		garrow_execute_plan_build_nested_loop_join_node(pcontext->plan, left, right,
														nest_loop_options, &error);

	if (error)
		elog(ERROR, "Failed to create the nestedloopjoin node, cause: %s", error->message);
	return nestedloopjoin_node;
}

GArrowSchema *
GetDummySchema(void)
{
	if (dummy_schema)
		return dummy_schema;
	// only call once
	g_autoptr(GArrowDataType) null_type = NULL;
	g_autoptr(GArrowField) field = NULL;
	GList *fields = NULL;
	null_type = GARROW_DATA_TYPE(garrow_null_data_type_new());
	field = garrow_field_new("dummy", null_type);
	fields = garrow_list_append_ptr(fields, field);
	dummy_schema = garrow_schema_new(fields);
	garrow_list_free_ptr(&fields);
	return dummy_schema;
}

void
FreeDummySchema(void) 
{
	if (dummy_schema)
		ARROW_FREE(GArrowSchema, &dummy_schema);
	dummy_schema = NULL;	
}

void 
dummy_schema_xact_cb(XactEvent event, void *arg)
{
	FreeDummySchema();
}
