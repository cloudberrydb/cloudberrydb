#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "access/formatter.h"
#include "catalog/pg_proc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/syscache.h"
#include "utils/int8.h"
#include "nodes/makefuncs.h"
#include "parser/parse_expr.h"
#include "catalog/pg_operator.h"
#include "optimizer/optimizer.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "rewrite/rewriteHandler.h"
#include "partition_selector.h"
#include "src/datalake_def.h"

typedef struct TypeInfoElt
{
	Oid  typeId;
	int  conTypeLen;
	bool conTypeByVal;
	Oid  conTypeId;
} TypeInfoElt;

typedef struct FuncInfoElt
{
	Oid  typeId;
	Oid  operId;
} FuncInfoElt;

static const TypeInfoElt typeInfoElts[] = {
	{INT2OID, 2, true, INT2OID},
	{INT4OID, sizeof(int32), true, INT4OID},
	{INT8OID, sizeof(int64), true, INT8OID},
	{FLOAT4OID, sizeof(int32), true, FLOAT4OID},
	{FLOAT8OID, sizeof(int64), true, FLOAT8OID},
	{NUMERICOID, -1, false, NUMERICOID},
	{TEXTOID, -1, false, TEXTOID},
	{BPCHAROID, -1, false, BPCHAROID}
};

static const FuncInfoElt funcInfoElts[] = {
	{INT2OID, Int2EqualOperator},
	{INT4OID, Int4EqualOperator},
	{INT8OID, Int8EqualOperator},
	{FLOAT4OID, Float4EqualOperator},
	{FLOAT8OID, Float8EqualOperator},
	{NUMERICOID, NumericEqualOperator},
	{TEXTOID, TextEqualOperator},
	{BPCHAROID, BpcharEqualOperator}
};

List * initializePartitionConstraint(List *partitionKeys,
							  List *partitionValues,
							  List *quals,
							  TupleDesc tupleDesc);

static void
lookupTypeInfo(Oid typeId,
		int *conTypeLen,
		bool *conTypeByVal,
		Oid *conTypeId)
{
	int i;

	for (i = 0; i < lengthof(typeInfoElts); i++)
	{
		if (typeInfoElts[i].typeId == typeId)
		{
			*conTypeLen = typeInfoElts[i].conTypeLen;
			*conTypeByVal = typeInfoElts[i].conTypeByVal;
			*conTypeId = typeInfoElts[i].conTypeId;

			return;
		}
	}

	elog(ERROR, "unrecognized type: %u", typeId);
}

static Oid 
lookupFuncInfo(Oid typeId)
{
	int i;

	for (i = 0; i < lengthof(funcInfoElts); i++)
	{
		if (funcInfoElts[i].typeId == typeId)
			return funcInfoElts[i].operId;
	}

	return InvalidOid;
}

static Var *
createVar(TupleDesc tupleDesc, Index varno, int partKeyAttNum)
{
	Var   *var;
	Oid    attType = tupleDesc->attrs[partKeyAttNum].atttypid;
	Oid    attCollId = tupleDesc->attrs[partKeyAttNum].attcollation;
	int32  attTypeMod = tupleDesc->attrs[partKeyAttNum].atttypmod;
	AttrNumber attNum = tupleDesc->attrs[partKeyAttNum].attnum;

	var = makeVar(varno, /* varno */
			attNum,
			attType,
			attTypeMod,
			attCollId,
			0 /* varlevelsup */);
	var->location = -1;

	return var;
}

static Const *
createConst(TupleDesc tupleDesc, int partKeyAttNum, const char *partKeyValue)
{
	Const *conExpr;
	Oid    conTypeId;
	Datum  conVal;
	int    conTypeLen;
	bool   conTypeByVal;
	Oid    conCollId = InvalidOid;
	Oid    attType = tupleDesc->attrs[partKeyAttNum].atttypid;
	char  *attName = NameStr(tupleDesc->attrs[partKeyAttNum].attname);

	switch (attType)
	{
		case INT2OID:
			conVal = DirectFunctionCall1(int2in, CStringGetDatum(partKeyValue));
			break;
		case INT4OID:
			conVal = DirectFunctionCall1(int4in, CStringGetDatum(partKeyValue));
			break;
		case INT8OID:
			conVal = DirectFunctionCall1(int8in, CStringGetDatum(partKeyValue));
			break;
		case FLOAT4OID:
			conVal = DirectFunctionCall1(float4in, CStringGetDatum(partKeyValue));
			break;
		case FLOAT8OID:
			conVal = DirectFunctionCall1(float8in, CStringGetDatum(partKeyValue));
			break;
		case NUMERICOID:
			conVal = DirectFunctionCall3(numeric_in,
										 CStringGetDatum(partKeyValue),
										 ObjectIdGetDatum(InvalidOid),
										 Int32GetDatum(-1));
			break;
		case TEXTOID:
			conVal = DirectFunctionCall1(textin, CStringGetDatum(partKeyValue));
			conCollId = tupleDesc->attrs[partKeyAttNum].attcollation;
			break;
		case BPCHAROID:
			conVal = DirectFunctionCall3(bpcharin,
										 CStringGetDatum(partKeyValue),
										 ObjectIdGetDatum(InvalidOid),
										 Int32GetDatum(-1));
			conCollId = tupleDesc->attrs[partKeyAttNum].attcollation;
			break;
		default:
			elog(ERROR, "does not support column:\"%s\" typeOid:\"%u\" as a partition key",
					attName, attType);
	}

	lookupTypeInfo(attType, &conTypeLen, &conTypeByVal, &conTypeId);

	conExpr = makeConst(conTypeId,
			-1, /* consttypmod */
			conCollId,
			conTypeLen,
			conVal,
			false, /* constisnull */
			conTypeByVal);
	conExpr->location = -1;

	return conExpr;
}

static Node *
createCheckConstraint(TupleDesc tupleDesc, Index varno, int partKeyAttNum, const char *partKeyValue)
{
	Var    *leftExpr;
	OpExpr *result;
	Const  *rightExpr;
	Oid     attType = tupleDesc->attrs[partKeyAttNum].atttypid;

	leftExpr = createVar(tupleDesc, varno, partKeyAttNum);
	rightExpr = createConst(tupleDesc, partKeyAttNum, partKeyValue);

	result = makeNode(OpExpr);
	result->opno = lookupFuncInfo(attType);
	result->opfuncid = InvalidOid;
	result->opresulttype = BOOLOID;
	result->opretset = false;
	result->args = list_make2(leftExpr, rightExpr);
	result->inputcollid = tupleDesc->attrs[partKeyAttNum].attcollation;
	result->location = -1;

	return (Node *) result;
}

int
getAttnumber(TupleDesc tupleDesc, const char *attName)
{
	int i;

	for (i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);
		if (pg_strcasecmp(NameStr(attr->attname), attName) == 0)
			return i;
	}

	return -1;
}

static List *
createCheckConstraints(TupleDesc tupleDesc, Index varno, List *partitionKeys, List *partitionValues)
{
	List *result = NIL;
	ListCell *lcKey;
	ListCell *lcValue;
	Node *conExpr;

	forboth(lcKey, partitionKeys, lcValue, partitionValues)
	{
		char *partKey = (char *) lfirst(lcKey);
		char *partValue = (char *) lfirst(lcValue);
		int   attNum = getAttnumber(tupleDesc, partKey);

		if (attNum == -1)
			elog(ERROR, "table doesn't have \"%s\" column, please make "
						"sure you have the right partition keys", partKey);

		conExpr = createCheckConstraint(tupleDesc, varno, attNum, partValue);

		conExpr = eval_const_expressions(NULL, conExpr);

		/*
		 * Finally, convert to implicit-AND format (that is, a List) and
		 * append the resulting item(s) to our output list.
		 */
		result = list_concat(result,
							 make_ands_implicit((Expr *) conExpr));
	}

	return result;
}

static List *
createDefaultConstraints(TupleDesc tupleDesc, List *partitionKeys, List *partitionValues)
{
	List *result = NIL;
	ListCell *lcKey;
	ListCell *lcValue;
	Expr *conExpr;

	forboth(lcKey, partitionKeys, lcValue, partitionValues)
	{
		char *partKey = (char *) lfirst(lcKey);
		char *partValue = (char *) lfirst(lcValue);
		int   attNum = getAttnumber(tupleDesc, partKey);

		if (attNum == -1)
			elog(ERROR, "table doesn't have \"%s\" column, please make "
						"sure you have the right partition keys", partKey);

		conExpr = (Expr *) createConst(tupleDesc, attNum, partValue);
		conExpr = expression_planner(conExpr);

		result = lappend(result, conExpr);
	}

	return result;
}

static bool
excludePartitions(List *quals, List *constraints)
{
	if (predicate_refuted_by(constraints, quals, false))
		return true;

	return false;
}

static void
moveArrayElem(char *value)
{
	int i;
	int len = strlen(value);

	if (len < 2)
		return;

	for (i = 1; i < len; i++)
		value[i - 1] = value[i];

	value[len - 2] = '\0';
}

static char *
unEscapeString(const char *value, int len, char deli, char escape)
{
	int   i;
	char *result = pnstrdup(value, len);

	for (i = 0; i < len; i++)
	{
		char curChar = result[i];
		char nextChar = result[i + 1];

		if (curChar != escape)
			continue;

		if (nextChar == deli || nextChar == escape)
			moveArrayElem(result + i);
	}

	return result;
}

List *
splitString2(const char *value, char deli, char escape)
{
	int    i;
	int    pos = 0;
	int    len = strlen(value);
	bool   isEscape;
	List  *result = NIL;

	for (i = 0; i < len; i++)
	{
		isEscape = false;
		if (value[i] == escape)
		{
			isEscape = true;
			i++;
		}

		if (!isEscape && value[i] == deli)
		{
			result = lappend(result, unEscapeString(value + pos, i - pos, deli, escape));
			pos = i + 1;
		}

		if (i == len - 1)
			result = lappend(result, unEscapeString(value + pos, len - pos, deli, escape));
	}

	return result;
}

static void
printPartition(List *partitionKeys, List *partitionValues)
{
	ListCell *lcKey;
	ListCell *lcValue;
	StringInfoData buff;

	initStringInfo(&buff);

	appendStringInfoString(&buff, "include partition ");

	forboth(lcKey, partitionKeys, lcValue, partitionValues)
	{
		char *partKey = (char *) lfirst(lcKey);
		char *partValue = (char *) lfirst(lcValue);

		appendStringInfo(&buff, "key:\"%s\" value:\"%s\",", partKey, partValue);
	}

	elog(DEBUG1, "%s", buff.data);

	pfree(buff.data);
}

typedef struct QualVarContext
{
	Index varno;
	int natts;
} QualVarContext;

static bool
qualVarWalker(Node *node, QualVarContext *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		if (IS_SPECIAL_VARNO(var->varno))
			return false;

		if (var->varattno >= 0 &&
			var->varattno <= ctx->natts &&
			var->varlevelsup == 0)
		{
			ctx->varno = var->varno;
			return true;
		}

		return false;
	}

	return expression_tree_walker(node, qualVarWalker, (void *) ctx);
}

static Index
getVarno(TupleDesc tupleDesc, List *quals)
{
	QualVarContext ctx;

	ctx.varno = 1;
	ctx.natts = tupleDesc->natts;

	qualVarWalker((Node *) quals, &ctx);

	return ctx.varno;
}

List *
selectPartitions(List *quals, TupleDesc tupleDesc, List *keys, List *partitions, bool allParts)
{
	ListCell *lc;
	List *conExpr;
	int i = 0;
	List *result = NIL;
	PartitionConstraint *partConst;
	Index varno = getVarno(tupleDesc, quals);

	foreach_with_count(lc, partitions, i)
	{
		List *partitionValues = (List *) lfirst(lc);

		if (!allParts)
		{
			conExpr = createCheckConstraints(tupleDesc, varno, keys, partitionValues);

			if (excludePartitions(quals, conExpr))
				continue;
		}

		printPartition(keys, partitionValues);

		partConst = palloc0(sizeof(PartitionConstraint));

		partConst->partitionValues = partitionValues;
		partConst->constraints = createDefaultConstraints(tupleDesc, keys, partitionValues);

		result = lappend(result, partConst);
	}

	return result;
}

List *
initializePartitionConstraint(List *partitionKeys,
							  List *partitionValues,
							  List *quals,
							  TupleDesc tupleDesc)
{
	List *result = NIL;

	if (partitionKeys == NIL)
		return result;

	if (quals == NULL)
	{
		result = selectPartitions(quals,
							tupleDesc,
							partitionKeys,
							partitionValues,
							true);
	}
	else
	{
		elog(DEBUG1, "datalake_fdw: filter quals is provided");
		result = selectPartitions(quals,
							tupleDesc,
							partitionKeys,
							partitionValues,
							false);
	}
	return result;
}

void initializeConstraints(dataLakeOptions *options, List *quals, TupleDesc tupleDesc)
{
	if (options->hiveOption->hivePartitionKey)
	{
		ListCell *lc;
		options->hiveOption->hivePartitionConstraints =
		initializePartitionConstraint(options->hiveOption->hivePartitionKey,
								options->hiveOption->hivePartitionValues,
								quals,
								tupleDesc);
		foreach(lc, options->hiveOption->hivePartitionKey)
		{
			char* partitionkey = (char*) lfirst(lc);
			int attNum = getAttnumber(tupleDesc, partitionkey);
			if (attNum == -1)
			{
				elog(ERROR,  "table doesn't have \"%s\" column, please make "
						"sure you have the right partition keys", partitionkey);
			}
			options->hiveOption->attNums = lappend_int(options->hiveOption->attNums, attNum + 1);
		}

		if (options->hiveOption->hivePartitionConstraints)
		{
			options->hiveOption->curPartition = 0;
		}
	}
}

bool
isLastPartition(void* sstate)
{
	dataLakeFdwScanState* scanstate = (dataLakeFdwScanState*) sstate;
	int nPartitions;

	if (scanstate->options->hiveOption->hivePartitionKey == NULL)
		return true;

	nPartitions = list_length(scanstate->options->hiveOption->hivePartitionConstraints);
	if (nPartitions == 0)
		return true;

	if (scanstate->options->hiveOption->curPartition == (nPartitions))
		return true;

	return false;
}

int
initializeDefaultMap(List *attNums,
					 List *constraints,
					 bool *proj,
					 int *defMap,
					 ExprState **defExprs)
{
	int nDefaults = 0;
	ListCell *lcAttNum;
	ListCell *lcCons;

	forboth(lcAttNum, attNums, lcCons, constraints)
	{
		int attNum = lfirst_int(lcAttNum);
		Expr *conExpr = (Expr *) lfirst(lcCons);

		if (proj[attNum - 1])
		{
			defExprs[nDefaults] = ExecInitExpr(conExpr, NULL);
			defMap[nDefaults] = attNum - 1;
			nDefaults++;
		}
	}

	return nDefaults;
}

List *
transfromHMSPartitions(List *partitions)
{
	ListCell *li;
	ListCell *lc;
	List *result = NIL;

	foreach(lc, partitions)
	{
		List *values = NIL;
		List *lValues = (List *) lfirst(lc);

		foreach(li, lValues)
		{
			char *iValues = strVal(lfirst(li));
			values = lappend(values, pstrdup(iValues));
		}

		result = lappend(result, values);
	}

	return result;
}

Datum
ExecEvalConst(ExprState *exprstate, ExprContext *econtext,
			  bool *isNull, ExprDoneCond *isDone)
{
	Const	   *con = (Const *) exprstate->expr;

	if (isDone)
		*isDone = ExprSingleResult;

	*isNull = con->constisnull;
	return con->constvalue;
}

Datum
ExecEvalConst2(ExprState *exprstate, ExprContext *econtext,
			  ExprDoneCond *isDone)
{
	Const	   *con = (Const *) exprstate->expr;

	if (isDone)
		*isDone = ExprSingleResult;

	return con->constvalue;
}
