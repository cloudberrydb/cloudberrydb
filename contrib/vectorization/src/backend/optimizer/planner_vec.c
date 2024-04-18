/*-------------------------------------------------------------------------
 *
 * planner_vec.c
 *	  The query vectorized optimizer external interface.
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 *
 * IDENTIFICATION
 *		src/backend/optimizer/planner_vec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "optimizer/planner.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbmutate.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/defrem.h"
#include "optimizer/cost.h"
#include "optimizer/planshare.h"
#include "optimizer/planmain.h"
#include "miscadmin.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "parser/parsetree.h"
#include "parser/scansup.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "nodes/nodes.h"
#include "cdb/cdbllize.h"
#include "optimizer/walkers.h"
#include "utils/guc.h"
#include "portability/instr_time.h"
#include "utils/fmgroids.h"
#include "port.h"
#include "utils/pg_locale.h"
#include "utils/syscache.h"

#include "optimizer/planner_vec.h"
#include "vecexecutor/executor.h"
#include "vecnodes/nodes.h"
#include "utils/fmgr_vec.h"
#include "utils/guc_vec.h"
#include "utils/wrapper.h"
#include "utils/fmgr_vec.h"

#include <sys/stat.h>
#include <sys/file.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define NUMERIC_EQ 1718
#define NUMERIC_NE 1719
#define NUMERIC_GT 1720
#define NUMERIC_GE 1721
#define NUMERIC_LT 1722
#define NUMERIC_LE 1723
#define NUMERIC_ADD 1724
#define NUMERIC_SUB 1725

planner_hook_type           planner_prev = NULL;

static void arrow_aggref_adapter(List *targetlist);
static void match_replace_tl(List *dst, List* src);
static Node *agg_trans_mutator(Node *node, void *context);
static Node *vectorize_plan_mutator(Node *node, void *context);
static bool is_plan_vectorable(Plan* plan, List *rtable);
static bool is_expr_vectorable(Expr* expr, void *context);
static bool is_hash_expr_vectorable(Expr* expr, void *context);
static bool is_relation_vectorable(SeqScan* seqscan, List *rtable, bool isForeign);
static bool is_sort_collation_vectorable(Sort *sort);
static bool fallback_distinct_junk(Plan *plan);
static bool fallback_nested_loop_jointype(Plan *plan);
static bool joinclauses_type_different(Expr *node, void *context);
static bool
is_foreign_scan_vectorable(ForeignScan *foreignscan);

static char* vector_foreign_data_wapper_whitelist[] =
{
	"datalake_fdw"
};

static bool
is_type_vectorable(Oid typeOid)
{
	if (PGTypeToArrowID(typeOid) > 0)
		return true;
	elog(DEBUG2, "Fallback to non-vectorization; type %d not support", typeOid);
	return false;
}

/* Fixme: Should check by OID instead of name. */
static bool
is_func_vectorable(Oid funcOid)
{
	if (get_function_name(funcOid) != NULL)
		return true;
	elog(DEBUG2, "Fallback to non-vectorization; funcOid %d not support", funcOid);
	return false;
}

static bool 
is_opexpr_vectorable(Oid operatorOid){
	                  
	if (get_op_name(operatorOid) != NULL)
		return true;
	elog(DEBUG2, "Fallback to non-vectorization; operatorOid %d not support", operatorOid);
	return false;

}

static bool 
is_gandiva_func_support(Oid opno)
{
	char *name = get_opname(opno);
	if (!name)
		return false;
	if (0 == strcmp(name, "+"))
		return true;
	else if (0 == strcmp(name, "-"))
		return true;
	else if (0 == strcmp(name, "*"))
		return true;
	else if (0 == strcmp(name, "/"))
		return true;
	return false;
}

static bool
is_aggfn_vectorable(Oid aggfnOid)
{
	if (get_arrow_fmgr(aggfnOid) != NULL)
		return true;
	elog(DEBUG2, "Fallback to non-vectorization; aggfnoid %d not support", aggfnOid);
	return false;
}

/*
 * Fixme: These types may appear in targetlist as intermediate type, but we
 * don't support the scan of this types. So we work around.
 * Remove this after all scanning type supported.
 */
static bool
is_scan_type_vectorable(Oid typeOid)
{
	if (typeOid == BYTEAOID || type_is_array(typeOid))
	{
		elog(DEBUG2, "Fallback to non-vectorization; scan type %d not support", typeOid);
		return false;
	}
	return is_type_vectorable(typeOid);
}

typedef struct
{
	plan_tree_base_prefix base; /* Required prefix for
								 * plan_tree_walker/mutator */
	int			nextPlanId;
} assign_plannode_id_context;

/* cbdb remove this */
static bool assign_plannode_id_walker(Node *node, assign_plannode_id_context *ctxt);

static bool
assign_plannode_id_walker(Node *node, assign_plannode_id_context *ctxt)
{
	if (node == NULL)
		return false;

	if (is_plan_node(node))
		((Plan *) node)->plan_node_id = ctxt->nextPlanId++;

	if (IsA(node, SubPlan))
		return false;

	return plan_tree_walker(node, assign_plannode_id_walker, ctxt, true);
}

/*
 * assign_plannode_id - Assign an id for each plan node.
 * Used by gpmon and instrument.
 */
static void
assign_plannode_id(PlannedStmt *stmt)
{
	assign_plannode_id_context ctxt;
	ListCell   *lc;

	ctxt.base.node = (Node *) stmt->planTree;
	ctxt.nextPlanId = 0;

	assign_plannode_id_walker((Node *) stmt->planTree, &ctxt);

	foreach(lc, stmt->subplans)
	{
		Plan	   *subplan = lfirst(lc);

		Assert(subplan);

		assign_plannode_id_walker((Node *) subplan, &ctxt);
	}
}

static PlannedStmt *
generate_plan(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;
	instr_time	starttime, endtime;
	extern planner_hook_type planner_prev;

	if (planner_prev) 
	{
		if (gp_log_optimization_time)
			INSTR_TIME_SET_CURRENT(starttime);

		result = (*planner_prev) (parse, query_string, cursorOptions, boundParams);

		if (gp_log_optimization_time)
		{
			INSTR_TIME_SET_CURRENT(endtime);
			INSTR_TIME_SUBTRACT(endtime, starttime);
			elog(LOG, "Planner Hook(s): %.3f ms", INSTR_TIME_GET_MILLISEC(endtime));
		}
	}
	else
	{
		result = standard_planner(parse, query_string, cursorOptions, boundParams);
	}

	return result;
}


PlannedStmt *
planner_hook_wrapper(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;
	PlannedStmt *origin_result;
	bool optimizer_origin;
	extern bool optimizer;
	extern bool force_vectorization;
	
	optimizer_origin = optimizer;

	result = generate_plan(parse, query_string, cursorOptions, boundParams);

	/* fallback for prepare and execute */
	/* fallback for cursor */
	if (!enable_vectorization  || \
			boundParams || cursorOptions != CURSOR_OPT_PARALLEL_OK)
		return result;
	
	init_vector_types();
	if (!try_vectorize_plan(result) && force_vectorization) {
		PG_TRY();
		{
			optimizer = !optimizer;
			origin_result = result;
			
			result = generate_plan(parse, query_string, cursorOptions, boundParams); 
		}
		PG_FINALLY();
		{
			optimizer = optimizer_origin;
			if(!try_vectorize_plan(result))
			{
				elog(DEBUG2, "Fallback to non-vectorization; current plan cannot be vectorized");
				result = origin_result;
			}
			else
			{
				elog(DEBUG2, "switch to another optimizer to generate vectorized plan");
			}
		}
		PG_END_TRY();
		

	}

	return result;
}

/*
 * Try to generate a vectorized plan.
 */
bool
try_vectorize_plan(PlannedStmt *result)
{
	bool vectorable;
	Plan *plan_copy;
	VectorExtensionContext *ext_ctx = NULL;

	plan_copy = copyObject(result->planTree);
	
	plan_copy = (Plan *) vectorize_plan_mutator((Node *) plan_copy, NULL);

	vectorable = is_plan_vectorable(plan_copy, result->rtable);
	if (!vectorable)
		return false;

	ext_ctx = (VectorExtensionContext *) palloc(sizeof(VectorExtensionContext));
	ext_ctx->base.type = T_ExtensibleNode;
	ext_ctx->base.extnodename = pstrdup(VECTOR_EXTENSION_CONTEXT);
	result->extensionContext = lappend(result->extensionContext, (ExtensibleNode *)ext_ctx);
	result->planTree = plan_copy;

	/* after vectorization, should reassign plan node id */
	assign_plannode_id(result);

	return true;
}


Node *
vectorize_plan_mutator(Node *node, void *context)
{
	Plan *mutated;

	if (node == NULL)
		return NULL;

	mutated = (Plan *)plan_tree_mutator(
		node, vectorize_plan_mutator, context, false);

	if (IsA(mutated, Agg))
		arrow_aggref_adapter(mutated->targetlist);

	/* Match scale of Numeric Const with var type*/
	if (IsA(mutated, OpExpr))
	{
		OpExpr *op = (OpExpr *)mutated;

		if ((op->opfuncid == NUMERIC_EQ) ||
				(op->opfuncid == NUMERIC_NE) ||
				(op->opfuncid == NUMERIC_GT) ||
				(op->opfuncid == NUMERIC_GE) ||
				(op->opfuncid == NUMERIC_LT) ||
				(op->opfuncid == NUMERIC_LE) ||
				(op->opfuncid == NUMERIC_ADD) ||
				(op->opfuncid == NUMERIC_SUB))
		{
			Expr *arg1 = (Expr *)linitial(op->args);
			Expr *arg2 = (Expr *)lsecond(op->args);
			Const *con = NULL;
			int32 typmod = 0;

			if (IsA(arg1, Const))
			{
				con = (Const *)arg1;
				typmod = exprTypmod((Node *)arg2);
				con->consttypmod = typmod;
			}
			else if (IsA(arg2, Const))
			{
				con = (Const *)arg2;
				typmod = exprTypmod((Node *)arg1);
				con->consttypmod = typmod;
			}
		}
	}

	if (IsA(mutated, NestLoop))
	{
		/* remove material node in inner. */
		if (mutated->righttree && IsA(mutated->righttree, Material))
			mutated->righttree = mutated->righttree->lefttree;
	}

	/* Modify table may contains vars didn't match children's targetlist*/
	if (is_plan_node((Node *) mutated) && mutated->lefttree
			&& mutated->lefttree->targetlist)
	{
		/* Fixme: Support join mutated */
		if (!mutated->righttree)
			match_replace_tl(mutated->targetlist, mutated->lefttree->targetlist);
		/* Fixme: replace for qual should be supported */
		//match_replace_tl(mutated->qual, mutated->lefttree->targetlist);
	}
	return (Node *) mutated;
}

static inline bool
is_expr_fallback(Expr* expr, void *context)
{
	return !is_expr_vectorable(expr, context);
}

/*
 * true for vectorized plan.
 * false for fallback.
 *
 * This walker reverse normal expression walker. Return true to continue walker,
 * return false to stop.
 */
static bool
is_expr_vectorable(Expr* expr, void *context)
{
	if (expr == NULL)
		return true;

	/* Find the vector engine not support expression */
	switch (nodeTag(expr))
	{
		case T_Var:
			{
				Var *var = (Var *) expr;

				if (!is_type_vectorable(var->vartype))
					return false;
			}
			break;
		case T_Const:
			{
				Const* c = (Const *)expr;

				if (!is_type_vectorable(c->consttype))
					return false;
			}
			break;
		case T_OpExpr:
			{
				OpExpr	  *opexpr = (OpExpr *)expr;

				if (!is_opexpr_vectorable(opexpr->opno) || list_length(opexpr->args) == 1)
					return false;
				if (!is_type_vectorable(opexpr->opresulttype))
					return false;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr	  *opexpr = (FuncExpr *)expr;

				if (!is_func_vectorable(opexpr->funcid))
					return false;
				if (!is_type_vectorable(opexpr->funcresulttype))
					return false;
			}
			break;
		case T_WindowFunc:
		case T_Aggref:
			{
				Oid typid  = InvalidOid;
				Oid funcid = InvalidOid;

				if (IsA(expr, Aggref))
				{
					Aggref *aggref = (Aggref *) expr;
					typid  = aggref->aggtype;
					funcid = aggref->aggfnoid;
					if(aggref->aggfilter != NULL)
					{
						elog(DEBUG2, "Fallback to non-vectorization; agg filter not support");
						return false;
					}			
				}
				else /* WindowFunc */
				{
					WindowFunc *wfunc = (WindowFunc *) expr;
					typid  = wfunc->wintype;
					funcid = wfunc->winfnoid;
					if(wfunc->aggfilter != NULL)
					{
						elog(DEBUG2, "Fallback to non-vectorization; agg filter not support");
						return false;
					}	
				}

				if (!is_type_vectorable(typid))
					return false;
				if (!is_aggfn_vectorable(funcid))
					return false;
			}
			break;
		case T_RelabelType:
			{
				RelabelType	  *relexpr = (RelabelType *)expr;

				if (!is_type_vectorable(relexpr->resulttype))
					return false;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *arrayexpr = (ScalarArrayOpExpr *) expr;

				/* Fixme: Fully support of ScalarArrayOpExpr, only "IN" now. */
				if (strcmp(get_opname(arrayexpr->opno), "=")
						|| !IsA(lsecond(arrayexpr->args), Const))
				{
					elog(DEBUG2, "Fallback to non-vectorization; ScalarArrayOpExpr not in");
					return false;
				}
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) expr;

				if (!is_type_vectorable(caseexpr->casetype))
					return false;
				break;
			}
		case T_Param:
			{
				Param *param = (Param *) expr;
				if (param->paramkind != PARAM_EXEC)
				{
					elog(DEBUG2, "Fallback to non-vectorization; PARAM only support PARAM_EXEC");
					return false;
				}
				break;
			}
		case T_NullIfExpr:
		/* Expressions fully vectorable */
		case T_CaseTestExpr:
		case T_NullTest:
		case T_BoolExpr:
		case T_List:
		case T_SortGroupClause:
		case T_TargetEntry:
		case T_CoalesceExpr:
		case T_DistinctExpr:
			break;
		/* All other expression fallback */
		default:
			elog(DEBUG2, "Fallback to non-vectorization; Expression not support %s",
					nodeToString(expr));
			return false;
			break;
	}

	/* reaching here means current expression is vectorable, continue walker */
	return !expression_tree_walker((Node *) expr, is_expr_fallback, context);
}

/*
 * @Description: Check if it has unsupport expression in vector engine
 *
 * true for vectorized plan.
 * false for fallback.
 */
static bool
is_plan_vectorable(Plan* plan, List *rtable)
{
	if (plan == NULL)
		return true;

	if (!rtable)
	{
		elog(DEBUG2, "Fallback to non-vectorization; Query without table.");
		return false;
	}

	if (!is_plan_vectorable(plan->lefttree, rtable))
		return false;

	if (!is_plan_vectorable(plan->righttree, rtable))
		return false;

	switch (nodeTag(plan))
	{
		/* Check plan node to fallback unvectorable cases: */
		case T_SeqScan:
			{
				SeqScan *seqscan = (SeqScan*)plan;

				if (!is_relation_vectorable((Scan *)seqscan, rtable, false))
					return false;
			}
			break;
		case T_Sort:
			{
				Sort *sort = (Sort *)plan;

				if (!is_sort_collation_vectorable(sort))
					return false;
			}
			break;
		case T_Result:
			{
				if (!plan->lefttree)
				{
					elog(DEBUG2, "Fallback to non-vectorization; Result without table.");
					return false;
				}
			}
			break;
		case T_Agg:
			{
				Agg *agg = (Agg *)plan;

				if (agg->aggstrategy == AGG_MIXED || agg->groupingSets
						|| (fallback_distinct_junk(plan)))
				{
					elog(DEBUG2, "Fallback to non-vectorization; Unsupported Agg type.");
					return false;
				}
				for (int i = 0; i < agg->numCols; i++)
				{
					if (agg->grpOperators[i] == ARRAY_EQ_OP)
					{
						elog(DEBUG2, "Fallback to non-vectorization; ARRAY_EQ_OP in agg.");
						return false;
					}
				}
			}
			break;
		case T_WindowAgg:
			{
				WindowAgg *wagg = (WindowAgg *)plan;

				int frameOptions = wagg->frameOptions;
				if (wagg->startOffset || wagg->endOffset ||
				    (frameOptions != FRAMEOPTION_DEFAULTS &&
				     frameOptions != (FRAMEOPTION_DEFAULTS | FRAMEOPTION_NONDEFAULT) &&
				     frameOptions != (FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS |
				                      FRAMEOPTION_START_UNBOUNDED_PRECEDING |
				                      FRAMEOPTION_END_CURRENT_ROW) &&
				     frameOptions != (FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS |
				                      FRAMEOPTION_BETWEEN |
				                      FRAMEOPTION_START_UNBOUNDED_PRECEDING |
				                      FRAMEOPTION_END_CURRENT_ROW)
				    )
				   )
				{
					elog(DEBUG2, "Fallback to non-vectorization; frameOption: %d with "
					             "\"frame\" clause not supported.", frameOptions);
					return false;
				}

				if (wagg->ordNumCols > 0)
				{
					Plan *child = plan;
					while (child && IsA(child, WindowAgg))
						child = child->lefttree;

					if (IsA(child, Sort))
					{
						Sort *sort = (Sort *)child;
						if (sort->numCols < wagg->ordNumCols)
						{
							elog(DEBUG2, "Fallback to non-vectorization; "
										"the number of sort keys: %d in the child Sort "
										"is less than the number of \"order by\" keys: %d in WindowAgg.",
										sort->numCols, wagg->ordNumCols);
							return false;
						}
					}
					else if (IsA(child, Motion))
					{
						Motion *motion = (Motion *)child;
						if (!motion->sendSorted)
						{
							elog(DEBUG2, "Fallback to non-vectorization; "
										"the motion node is NOT sorted.");
							return false;
						}
					}
					else if (IsA(child, Agg))
					{
						Agg *agg = castNode(Agg, child);

						if (agg->aggstrategy != AGG_SORTED)
						{
							elog(DEBUG2, "Fallback to non-vectorization; "
										"the agg node is NOT sorted.");
							return false;
						}

						Plan *child_of_agg = child->lefttree;
						if (!IsA(child_of_agg, Motion) && !IsA(child_of_agg, Sort))
						{
							elog(DEBUG2, "Fallback to non-vectorization; "
										"the child node of GroupAgg is NOT Sort or sorted GatherMotion.");
							return false;
						}
					}
					else
					{
						elog(DEBUG2, "Fallback to non-vectorization; "
									"the child node of the WindowAgg is NOT Sort or sorted GatherMotion.");
						return false;
					}
				}

				for (int i = 0; i < wagg->partNumCols; i++)
				{
					if (wagg->partOperators[i] == ARRAY_EQ_OP)
					{
						elog(DEBUG2, "Fallback to non-vectorization; ARRAY_EQ_OP in window agg.");
						return false;
					}
				}

				break;
			}
		case T_HashJoin:
			{
				HashJoin* hash_expr = (HashJoin *)plan;
				if (!is_expr_vectorable((Expr *)hash_expr->join.joinqual, NULL))
				{
					elog(DEBUG2, "Fallback to non-vectorization; Unsupported join qual expr.");
					return false;
				}
				if (!is_expr_vectorable((Expr *)hash_expr->hashkeys, NULL) || joinclauses_type_different((Expr *)hash_expr->hashclauses, NULL))
				{
					elog(DEBUG2, "Fallback to non-vectorization; Unsupported hash key expr.");
					return false;
				}
				if (!is_expr_vectorable((Expr *)hash_expr->hashqualclauses, NULL) || !is_expr_vectorable((Expr *)hash_expr->hashclauses, NULL) || joinclauses_type_different((Expr *)hash_expr->hashclauses, NULL))
				{
					elog(DEBUG2, "Fallback to non-vectorization; Unsupported hash clauses expr.");
					return false;
				}
			}
			break;
		case T_NestLoop:	
			{
				NestLoop *tstlp = (NestLoop *) plan;
				if (fallback_nested_loop_jointype(plan) || joinclauses_type_different((Expr *) tstlp->join.joinqual, NULL))
				{
					elog(DEBUG2, "Fallback to non-vectorization; Unsupported nested join type.");
					return false;
				}
			}
			break;
		case T_Motion:
			{
				Motion *motion = (Motion *)plan;

				if (!is_hash_expr_vectorable((Expr *)motion->hashExprs, NULL))
				{
					elog(DEBUG2, "Fallback to non-vectorization; hash expr support.");
					return false;
				}
			}
			break;
		case T_Limit:
			{
				Limit *node = (Limit *)plan;

				if (node->limitOption == LIMIT_OPTION_WITH_TIES)
				{
					elog(DEBUG2, "Fallback to non-vectorization;"
							" LIMIT_OPTION_WITH_TIES is not support.");
					return false;
				}
				if (node->limitCount && !IsA(node->limitCount, Const))
				{
					elog(DEBUG2, "Fallback to non-vectorization;"
							" LIMIT only support const expr");
					return false;
				}
				if (node->limitOffset && !IsA(node->limitOffset, Const))
				{
					elog(DEBUG2, "Fallback to non-vectorization;"
							" LIMIT only support const expr");
					return false;
				}
			}
			break;
		case T_SubqueryScan:
			{
				SubqueryScan *subquery = (SubqueryScan *)plan;
				if (!is_plan_vectorable(subquery->subplan, rtable))
						return false;
			}
			break;
		case T_Append:
			{
				ListCell   *lc;
				Append *append = (Append*) plan;
				foreach (lc, append->appendplans)
				{
					Plan *initNode = (Plan *)lfirst(lc);
					if (!is_plan_vectorable(initNode, rtable))
					{
						elog(DEBUG2, "Fallback to non-vectorization; Append subnode.");	
						return false;
					}
				}
			}
			break;
		case T_Sequence:
			{
				ListCell   *lc;
				Sequence *sequence = (Sequence *) plan;
				foreach (lc, sequence->subplans)
				{
					Plan *initNode = (Plan *)lfirst(lc);
					if (!is_plan_vectorable(initNode, rtable))
					{
						elog(DEBUG2, "Fallback to non-vectorization; Sequence subplan.");	
						return false;
					}
				}
			}
			break;
		case T_ForeignScan:
			{
				ForeignScan *foreignscan = (ForeignScan *) plan;

				if (!is_foreign_scan_vectorable(foreignscan))
					return false;
				if (!is_relation_vectorable((Scan *)foreignscan, rtable, true))
					return false;
			}
			break;

		/* Plans fully vectorable: */
		case T_AssertOp:
		case T_Hash:
		case T_ShareInputScan:
		case T_Material:
			break;
			break;
		/* All other plan node fallback */
		default:
			{
				elog(DEBUG2, "Fallback to non-vectorization, node is not supported, tag is : %d", nodeTag(plan));	
				return false;
			}		
	}

	return is_expr_vectorable((Expr *)plan->targetlist, NULL)
			&& is_expr_vectorable((Expr *)plan->qual, NULL);
}

static bool
is_foreign_scan_vectorable(ForeignScan *foreignscan)
{
	int i;
	ForeignServer *server;
	ForeignDataWrapper *fdw;

	server = GetForeignServer(foreignscan->fs_server);
	fdw = GetForeignDataWrapper(server->fdwid);

	for (i = 0; i < sizeof(vector_foreign_data_wapper_whitelist)/ sizeof(char *); i++)
		if (strcmp(vector_foreign_data_wapper_whitelist[i], fdw->fdwname) == 0)
			return true;
	elog(DEBUG2, "Fallback to non-vectorization, unsupported foreign data wrapper: %s", fdw->fdwname);
	return false;
}

static Node *
agg_trans_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	switch (nodeTag(node))
	{
		case T_Aggref:
		{
			Aggref *aggref = (Aggref *) node;
			if (aggref->aggsplit != AGGSPLIT_INITIAL_SERIAL)
				return node;
			switch (aggref->aggfnoid)
			{
				/* change result bytea type to numeric/decimal */
				case F_SUM_NUMERIC:
				case F_SUM_INT8:
				{
					aggref->aggtype = NUMERICOID;
					break;
				}
				case F_AVG_INT8:
				case F_AVG_NUMERIC:
				{
					/* change result bytea type to avgintbytea */
					aggref->aggtype = AvgIntByteAOid;
					break;
				}
			    case F_STDDEV_SAMP_INT4:
				{
					aggref->aggtype = STDDEVOID;
					break;
				}
			}
			return node;
		}
		default:
			break;
	}
	return expression_tree_mutator(node, agg_trans_mutator, context);
}


/*
 * sum(int2):
 *    trans: input var type is 21(INT2OID), result type(aggtype) is 20(INT8OID).
 *    final: input var type is 20(INT8OID), result type(aggtype) is 20(INT8OID).
 * 
 * avg(int2):
 *    trans: input var type is 21(INT2OID), result type(aggtype) is 1016(INT8ARRAYOID).
 *    final: input var type is 1016(INT8ARRAYOID), result type(aggtype) is 1700(NUMERICOID).
 *
 * sum(int4):
 *    trans: input var type is 23(INT4OID), result type(aggtype) is 20(INT8OID).
 *    final: input var type is 20(INT8OID), result type(aggtype) is 20(INT8OID).
 * 
 * avg(int4):
 *    trans: input var type is 23(INT4OID), result type(aggtype) is 1016(INT8ARRAYOID).
 *    final: input var type is 1016(INT8ARRAYOID), result type(aggtype) is 1700(NUMERICOID).
 * 
 * sum(int8):
 *    trans: input var type is 20(INT8OID), result type(aggtype) is 17(BYTEAOID).
 *    final: input var type is 17(BYTEAOID), result type(aggtype) is 1700(NUMERICOID).
 * 
 * avg(int8):
 *    trans: input var type is 20(INT8OID), result type(aggtype) is 17(BYTEAOID).
 *    final: input var type is 17(BYTEAOID), result type(aggtype) is 1700(NUMERICOID).
 * 
 */
static void
arrow_aggref_adapter(List *targetlist)
{
	ListCell *lc;
	foreach(lc, targetlist)
	{
		TargetEntry *tle = lfirst(lc);
		Node *node = (Node *) tle->expr;
		agg_trans_mutator(node, NULL);
	}
}

static bool 
is_relation_vectorable(Scan* seqscan, List *rtable, bool isForeign)
{
	TupleDesc tupdesc;
	AttrNumber	attnum;
	bool unsupport;
	StdRdOptions **opts;
	char *compresstype;
	unsupport = false;
	Plan *plan = (Plan *) seqscan;
	int *columnmap;
	RangeTblEntry *rte;
	Relation rel;
	TupleDesc reldesc;

	rte = rt_fetch(seqscan->scanrelid, rtable);
	/* Invalid table fallback */
	if ((rte == NULL) || (rte->rtekind != RTE_RELATION))
	{
		elog(DEBUG2, "Fallback to non-vectorization; Invalid relation");
		return false;
	}

	rel = relation_open(rte->relid, AccessShareLock);
	/* Fixme : Support more table, only aocs now. */
	if (isForeign)
	{
		ForeignTable *table;
		ListCell   *lc;

		table = GetForeignTable(rte->relid);
		foreach(lc, table->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "transactional") == 0 && defGetBoolean(def))
			{
				elog(DEBUG2, "Fallback to non-vectorization;"
						" foreign table relation is transactional.");
				return false;
			}
			if (strcmp(def->defname, "format") == 0 && strcmp(defGetString(def), "orc") != 0)
			{
				elog(DEBUG2, "Fallback to non-vectorization;"
						" foreign table relation is not orc.");
				return false;
			}
		}
	}
	else if (!(table_scan_flags(rel) & SCAN_SUPPORT_VECTORIZATION))
	{
		elog(DEBUG2, "Fallback to non-vectorization; relation does not support vectorization.");
		relation_close(rel, AccessShareLock);
		return false;
	}

	reldesc = RelationGetDescr(rel);
	columnmap = palloc0((reldesc->natts + 1) * sizeof(int));
	tupdesc = BuildDescForScanExtract(reldesc,
					    plan->targetlist,
					    plan->qual, columnmap);


	opts = RelationGetAttributeOptions(rel);
	relation_close(rel, AccessShareLock);
	if (!opts)
		return false;
	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

		if (!is_scan_type_vectorable(attr->atttypid))
			return false;

		/* no opts for pax and fdw, skip compresstype checking */
		if(!opts[columnmap[attnum - 1]])
			continue;
		compresstype = opts[columnmap[attnum - 1]]->compresstype;

		/* FIXME: fix this bug to support rle_type */
		if (strcmp(compresstype, "rle_type") == 0)
		{
			elog(DEBUG2, "Fallback to non-vectorization; relation is rel_type compressed.");
			return false;
		}
	}

	pfree(columnmap);
	return true;
}

static bool
is_sort_collation_vectorable(Sort *sort)
{
	if (sort->collations)
	{
		Oid *sortCollations = sort->collations;
		for (int i = 0; i < sort->numCols; i++)
		{
			Oid collation = sortCollations[i];
			HeapTuple  tp;
			Form_pg_collation collform;
			const char *collcollate;
			const char *localeptr = NULL;

			if (!OidIsValid(collation))
				continue;

			tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
			if (!HeapTupleIsValid(tp))
				elog(ERROR, "cache lookup failed for collation %u", collation);

			collform = (Form_pg_collation) GETSTRUCT(tp);
			collcollate = NameStr(collform->collcollate);

			if (strcasecmp(collcollate, "en_US.UTF-8") == 0
					|| strcasecmp(collcollate, "en_US.utf8") == 0
					|| (collform->oid == C_COLLATION_OID))
			{
				ReleaseSysCache(tp);
				continue;
			}
			if (collform->oid == DEFAULT_COLLATION_OID)
			{
				localeptr = setlocale(LC_COLLATE, NULL);
				if (!localeptr)
					elog(ERROR, "invalid LC_COLLATE setting");
				if (strcmp(localeptr, "C") == 0
						|| strcasecmp(localeptr, "en_US.utf8") == 0
						|| strcasecmp(localeptr, "en_US.UTF-8") == 0)
				{
					ReleaseSysCache(tp);
					continue;
				}
			}

			elog(DEBUG2, "Fallback to non-vectorization; Unsupported collation %d : %s. ",
					collform->oid, localeptr);
			ReleaseSysCache(tp);
			return false;
		}
	}
	return true;
}

static bool
fallback_nested_loop_jointype(Plan *plan) 
{
	NestLoop *node = (NestLoop *) plan;
	if (node->join.jointype == JOIN_LASJ_NOTIN)
		return true;
	return false;
}

static Node *
match_replace_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;
	TupleDesc tupdesc = (TupleDesc) context;
	switch (nodeTag(node))
	{
		case T_Var:
		{
			Var *var = (Var *) node;
			Form_pg_attribute attr;

			/* no need to match system columns */
			if (var->varattno < 0 || var->varattno > tupdesc->natts)
				break;
			/* Fixme: var->varattno zero represents all attributes.
			 */
			if (var->varattno == 0)
				break;

			attr = TupleDescAttr(tupdesc, var->varattno - 1);
			var->vartype = attr->atttypid;
			break;
		}
		/* Fixme: match for other expression type. */
		default:
			elog(DEBUG2, "[match replace tl] node type: %d not support", nodeTag(node));
	}
	return expression_tree_mutator(node, match_replace_mutator, context);
}

/*
 * The var of the current node is replaced with the var of the downstream node.
 */
static void
match_replace_tl(List *dst, List* src) 
{
	ListCell *lc;
	TupleDesc tupdesc;

	tupdesc = ExecTypeFromTL(src);
	foreach (lc, dst) 
	{
		TargetEntry *tle = lfirst(lc);
		Node *node = (Node *) tle->expr;
		match_replace_mutator(node, tupdesc);
	}
	FreeTupleDesc(tupdesc);
}

static inline bool
is_hash_expr_fallback(Expr* expr, void *context)
{
	return !is_hash_expr_vectorable(expr, context);
}

static bool
is_hash_expr_vectorable(Expr *expr, void *context)
{
	if (expr == NULL)
		return true;

	switch (nodeTag(expr))
	{
		/* motion expression white list*/
		case T_Var:
			{
				Var *var = (Var *) expr;

				if (!is_type_vectorable(var->vartype))
					return false;
			}
			break;
		case T_Const:
			{
				Const  *con = (Const *) expr;

				/* support types */
				if ((con->consttype == INT4OID)
						|| (con->consttype == INT8OID)
						|| (con->consttype == DATEOID)
						|| (con->consttype == TEXTOID))
					break;
				elog(DEBUG2, "Fallback to non-vectorization;"
						" Hash expression const type not support %d",
						con->consttype);
				return false;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr *bexpr = (BoolExpr *)expr;

				if (bexpr->boolop == AND_EXPR || bexpr->boolop == OR_EXPR)
					break;
				elog(DEBUG2, "Fallback to non-vectorization;"
							 " Hash expression bool expr type not support %d",
					 bexpr->boolop);
				return false;
			}
			break;
		/* Expressions fully vectorable */
		case T_List:
		case T_TargetEntry:
			break;
		case T_OpExpr:
			{
				OpExpr *opexpr = (OpExpr *) expr;
				if (!is_gandiva_func_support(opexpr->opno))
				{
					elog(DEBUG2, "converting invalid opexpr to gandiva node, func id : %d",
						(int) opexpr->opno);
					return false;
				}
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr *opexpr = (FuncExpr *)expr;
				if (opexpr->funcid != F_SUBSTR_TEXT_INT4_INT4 && opexpr->funcid != F_UPPER_TEXT)
				{
					elog(DEBUG2, "converting invalid funcexpr to gandiva node, func id : %d",
						(int) opexpr->funcid);
					return false;
				}
			}
			break;
		default:
			elog(DEBUG2, "Fallback to non-vectorization;"
					" Hash expression not support %s", nodeToString(expr));
			return false;
	}

	/* reaching here means current expression is vectorable, continue walker */
	return !expression_tree_walker((Node *) expr, is_hash_expr_fallback, NULL);
}

static bool
fallback_distinct_junk(Plan *plan)
{
	Agg *agg = (Agg *) plan;
	int grpkey = agg->numCols;
	List *tl = plan->targetlist;
	ListCell *l;
	bool has_aggref = false;
	bool has_junk_column = false;
	foreach (l, tl)
	{
		TargetEntry *tle = (TargetEntry *)lfirst(l);
		if (tle->resjunk)
			has_junk_column = true;
		Expr *node = tle->expr;
		switch (nodeTag(node))
		{
			case T_Aggref:
			{
				has_aggref = true;
				break;
			}
			default:
				break;
		}
	}
	return grpkey > 0 && !has_aggref && has_junk_column;
}

/*
 * true means different type
 * false means same type
 */
static bool
joinclauses_type_different(Expr *node, void *context)
{
	if (node == NULL)
		return false;

	ListCell *lc;
	Oid prev = InvalidOid;

	switch (nodeTag(node))
	{
	case T_List:
		{
			List *l = (List *)node;
			int length = list_length(l);
			foreach (lc, l)
			{
				Expr *expr = (Expr *)lfirst(lc);
				if (length == 1)
					return joinclauses_type_different(expr, context);
				else
				{
					Oid curr_oid = exprType((Node *)expr);
					if (prev != InvalidOid && prev != curr_oid)
						return true;
					prev = curr_oid;
				}
			}
			return false;
		}
		break;
	case T_OpExpr:
		{
			OpExpr *opexpr = (OpExpr *) node;
			return joinclauses_type_different((Expr *) opexpr->args, context);
		}
	default:
		break;
	}
	return expression_tree_walker((Node *) node, joinclauses_type_different, context);
}
