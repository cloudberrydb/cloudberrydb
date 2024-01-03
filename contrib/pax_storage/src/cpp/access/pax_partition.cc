#include "access/pax_partition.h"

#include "comm/cbdb_api.h"

#include "access/pax_access_handle.h"
#include "catalog/pg_pax_tables.h"
#include "comm/cbdb_wrappers.h"

namespace paxc {
// support optional `EVERY` syntax:
// FROM(start_value) TO(end_value) [ EVERY(interval) ]
struct PaxPartitionEveryIterator {
  PartitionKey key;
  Datum from_value;
  Datum to_value;

  ExprState *plus_expr_state;
  ParamListInfo plus_expr_params;
  EState *estate;

  Datum current_start;
  Datum current_end;
  bool ended;

  ParseState *pstate;
};

static int PartitionCheckBound(PartitionKey key, PartitionBoundSpec *spec);

static void PaxPartitionDestroyEveryIterator(
    struct PaxPartitionEveryIterator *iter) {
  if (iter->estate) FreeExecutorState(iter->estate);
  pfree(iter);
}

// See the implementation in PartEveryIterator
static struct PaxPartitionEveryIterator *PaxPartitionInitEveryIterator(
    ParseState *pstate, PartitionKey key, Node *from, Node *to, Node *every) {
  Assert(key->partnatts == 1);
  auto part_col_typid = get_partition_col_typid(key, 0);
  auto part_col_typmod = get_partition_col_typmod(key, 0);
  auto part_col_collation = get_partition_col_collation(key, 0);
  Datum from_value;
  Datum to_value;
  Const *c;

  auto iter =
      (PaxPartitionEveryIterator *)palloc0(sizeof(PaxPartitionEveryIterator));
  Assert(from && to && every);

  c = castNode(Const, from);
  if (c->constisnull)
    elog(ERROR, "cann't use NULL with range partition specification");
  from_value = c->constvalue;

  c = castNode(Const, to);
  if (c->constisnull)
    elog(ERROR, "cann't use NULL with range partition specification");
  to_value = c->constvalue;

  auto param = makeNode(Param);
  param->paramid = 1;
  param->paramtype = part_col_typid;
  param->paramtypmod = part_col_typmod;
  param->paramcollid = part_col_collation;
  param->location = -1;

  auto plus_expr = (Node *)make_op(
      pstate,
      list_make2(makeString((char *)"pg_catalog"), makeString((char *)"+")),
      (Node *)param, (Node *)every, pstate->p_last_srf, -1);

  if (IsA(plus_expr, CollateExpr)) {
    auto expr_collation = exprCollation(plus_expr);
    if (OidIsValid(expr_collation) && expr_collation != part_col_collation)
      elog(ERROR,
           "collation of partition bound value for column %d doesn't match "
           "partition key collation \"%s\"",
           get_partition_col_attnum(key, 0),
           get_collation_name(part_col_collation));
  }
  plus_expr = coerce_to_target_type(
      pstate, plus_expr, exprType(plus_expr), part_col_typid, part_col_typmod,
      COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
  if (plus_expr == NULL)
    ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             errmsg("specified value cannot be cast to type %s for column %d",
                    format_type_be(part_col_typid),
                    get_partition_col_attnum(key, 0))));

  iter->key = key;
  iter->from_value = from_value;
  iter->to_value = to_value;

  iter->plus_expr_params = makeParamList(1);
  iter->plus_expr_params->params[0].value = (Datum)0;
  iter->plus_expr_params->params[0].isnull = true;
  iter->plus_expr_params->params[0].pflags = 0;
  iter->plus_expr_params->params[0].ptype = part_col_typid;
  iter->estate = CreateExecutorState();
  iter->estate->es_param_list_info = iter->plus_expr_params;

  iter->plus_expr_state =
      ExecInitExprWithParams((Expr *)plus_expr, iter->plus_expr_params);

  iter->current_end = iter->from_value;
  iter->current_start = (Datum)0;
  iter->ended = false;

  iter->pstate = pstate;

  return iter;
}

static List *PaxPartitionBuildDatums(PartitionKey key, Datum *datums) {
  List *result = NIL;
  for (int i = 0; i < key->partnatts; i++) {
    Const *c;
    PartitionRangeDatum *prd;
    c = makeConst(
        key->parttypid[i], key->parttypmod[i], key->parttypcoll[i],
        key->parttyplen[i],
        datumCopy(datums[i], key->parttypbyval[i], key->parttyplen[i]), false,
        key->parttypbyval[i]);

    prd = makeNode(PartitionRangeDatum);
    prd->kind = PARTITION_RANGE_DATUM_VALUE;
    prd->value = (Node *)c;
    result = lappend(result, prd);
  }
  return result;
}

static PartitionBoundSpec *PaxPartitionNextPartBound(
    struct PaxPartitionEveryIterator *iter) {
  if (iter->ended) return nullptr;

  bool isnull;

  iter->plus_expr_params->params[0].isnull = false;
  iter->plus_expr_params->params[0].value = iter->current_end;

  auto next_start = iter->current_end;
  auto next_end = ExecEvalExprSwitchContext(
      iter->plus_expr_state, GetPerTupleExprContext(iter->estate), &isnull);
  if (isnull)
    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("could not compute next partition boundary with "
                           "EVERY, plus-operator returned NULL"),
                    parser_errposition(iter->pstate, -1)));

  auto cmpval = DatumGetInt32(FunctionCall2Coll(&iter->key->partsupfunc[0],
                                                iter->key->partcollation[0],
                                                next_end, iter->to_value));
  if (cmpval >= 0) {
    iter->ended = true;
    next_end = iter->to_value;
  }
  // sanity check in case next_start >= next_end
  cmpval = DatumGetInt32(FunctionCall2Coll(&iter->key->partsupfunc[0],
                                           iter->key->partcollation[0],
                                           next_start, next_end));
  if (cmpval >= 0) elog(ERROR, "invalid range bound with EVERY");

  iter->current_start = next_start;
  iter->current_end = next_end;

  // build PartitionBoundSpec for [iter->current_start, iter->current_end)
  PartitionBoundSpec *boundspec;

  boundspec = makeNode(PartitionBoundSpec);
  boundspec->strategy = PARTITION_STRATEGY_RANGE;
  boundspec->is_default = false;
  boundspec->lowerdatums =
      PaxPartitionBuildDatums(iter->key, &iter->current_start);
  boundspec->upperdatums =
      PaxPartitionBuildDatums(iter->key, &iter->current_end);

  return boundspec;
}

static Node *GetConstValue(List *datums) {
  auto prd = (PartitionRangeDatum *)linitial(datums);
  Assert(IsA(prd, PartitionRangeDatum));
  Assert(prd->kind == PARTITION_RANGE_DATUM_VALUE);

  auto c = (Const *)prd->value;
  Assert(c && IsA(c, Const) && !c->constisnull);
  return (Node *)c;
}

// generate a list of partition bound specs
static List *TransformPartitionExtension(ParseState *pstate, Relation relation,
                                         PartitionKey key,
                                         PartitionRangeExtension *range_ext) {
  List *result = NIL;
  PartitionBoundSpec *range;

  auto every = range_ext->every;
  auto spec = transformPartitionBound(pstate, relation, key, &range_ext->spec);
  if (!every) return list_make1(spec);

  if (PartitionCheckBound(key, spec) >= 0)
    elog(ERROR, "invalid range bound: from %s to %s every(X)",
         get_range_partbound_string(spec->lowerdatums),
         get_range_partbound_string(spec->upperdatums));

  // calculate partition by every expression
  if (key->partnatts != 1 || key->partnatts != list_length(every))
    elog(ERROR, "pax partition EVERY only support one column");

  auto ev = (Node *)linitial(every);
  auto iter = PaxPartitionInitEveryIterator(
      pstate, key, GetConstValue(spec->lowerdatums),
      GetConstValue(spec->upperdatums),
      (Node *)transformExpr(pstate, ev, EXPR_KIND_PARTITION_BOUND));

  while ((range = PaxPartitionNextPartBound(iter))) {
    result = lappend(result, range);
  }
  PaxPartitionDestroyEveryIterator(iter);
  return result;
}

static bool PaxLoadPartitionSpec(Oid relid, List **partparams_list,
                                 List **partboundspec_list) {
  Node *part;
  List *list;

  ::paxc::GetPaxTablesEntryAttributes(relid, NULL, &part);
  if (!part) return false;

  list = castNode(List, part);
  Assert(list_length(list) == 2);
  *partparams_list = castNode(List, list_nth(list, 0));
  *partboundspec_list = castNode(List, list_nth(list, 1));
  return true;
}

static inline PartitionRangeDatumKind RangeDatumToKind(List *datums, int i) {
  PartitionRangeDatum *rd = castNode(PartitionRangeDatum, list_nth(datums, i));
  return rd->kind;
}
static inline Datum RangeDatumToValue(List *datums, int i) {
  PartitionRangeDatum *rd = castNode(PartitionRangeDatum, list_nth(datums, i));
  Const *c = castNode(Const, rd->value);
  Assert(c && !c->constisnull);
  return c->constvalue;
}
// Reference: partition_rbound_cmp()
int PartitionComparePartitionKeys(PartitionKey key, List *datums1,
                                  List *datums2) {
  Assert(key->partnatts == list_length(datums1));
  Assert(key->partnatts == list_length(datums2));
  FmgrInfo *partsupfunc = key->partsupfunc;
  Oid *partcollation = key->partcollation;
  int natts = key->partnatts;
  int i;
  int32 colnum = 0;
  int32 cmpval = 0;
  for (i = 0; i < natts; i++) {
    colnum++;
    auto kind1 = RangeDatumToKind(datums1, i);
    auto kind2 = RangeDatumToKind(datums2, i);

    if (kind1 < kind2) return -colnum;
    if (kind1 > kind2) return colnum;
    if (kind1 != PARTITION_RANGE_DATUM_VALUE) {
      /*
       * The column bounds are both MINVALUE or both MAXVALUE. No later
       * columns should be considered, but we still need to compare
       * whether they are upper or lower bounds.
       */
      break;
    }
    cmpval = DatumGetInt32(FunctionCall2Coll(&partsupfunc[i], partcollation[i],
                                             RangeDatumToValue(datums1, i),
                                             RangeDatumToValue(datums2, i)));
    if (cmpval != 0) break;
  }
  return cmpval == 0 ? 0 : (cmpval < 0 ? -colnum : colnum);
}

static int PartitionCheckBound(PartitionKey key, PartitionBoundSpec *spec) {
  return PartitionComparePartitionKeys(key, spec->lowerdatums,
                                       spec->upperdatums);
}

int PartitionBoundSpecCmp(const ListCell *a, const ListCell *b, void *arg) {
  auto spec1 = lfirst_node(PartitionBoundSpec, a);
  auto spec2 = lfirst_node(PartitionBoundSpec, b);
  auto key = static_cast<PartitionKey>(arg);
  return PartitionComparePartitionKeys(key, spec1->lowerdatums,
                                       spec2->lowerdatums);
}

bool PartitionCheckBounds(PartitionKey key, List *spec_list) {
  ListCell *lc;
  int i;
  int nparts = list_length(spec_list);
  bool ok = true;

  Assert(nparts > 0);

  // self bound check
  foreach (lc, spec_list) {
    auto spec = lfirst_node(PartitionBoundSpec, lc);

    if (spec->strategy != key->strategy)
      elog(ERROR, "strategy not match with partition key");
    if (spec->is_default) elog(ERROR, "unexpected default partition");
    if (list_length(spec->lowerdatums) != key->partnatts)
      elog(ERROR,
           "number of lower bound values mismatches the number of partition "
           "keys");
    if (list_length(spec->upperdatums) != key->partnatts)
      elog(ERROR,
           "number of upper bound values mismatches the number of partition "
           "keys");

    ok = PartitionCheckBound(key, spec) < 0;
    if (!ok) goto out;
  }

  // cross bound check, only checks whether prev.upper <= cur.lower
  list_sort_arg(spec_list, PartitionBoundSpecCmp, key);
  for (i = 1; i < nparts; i++) {
    auto spec1 = castNode(PartitionBoundSpec, list_nth(spec_list, i - 1));
    auto spec2 = castNode(PartitionBoundSpec, list_nth(spec_list, i));
    // the upper value should be less than or equal to the lower value of the
    // next part
    ok = PartitionComparePartitionKeys(key, spec1->upperdatums,
                                       spec2->lowerdatums) <= 0;
    if (!ok) break;
  }
out:
  return ok;
}

List *PaxValidatePartitionRanges(Relation relation, PartitionKey key,
                                 List *raw_partbound_list) {
  ParseState *pstate = make_parsestate(NULL);
  List *spec_list = NIL;
  int nparts;
  bool ok;

  nparts = list_length(raw_partbound_list);
  Assert(nparts > 0);

  for (int i = 0; i < nparts; i++) {
    auto spec =
        static_cast<PartitionRangeExtension *>(list_nth(raw_partbound_list, i));
    Assert(IsA(spec, PartitionBoundSpec));
    auto part_list = TransformPartitionExtension(pstate, relation, key, spec);
    spec_list = list_concat(spec_list, part_list);
    pfree(part_list);
  }

  // check whether bounds overlaps
  ok = paxc::PartitionCheckBounds(key, spec_list);
  if (!ok) elog(ERROR, "partition bounds have overlaps");

  list_free_deep(raw_partbound_list);
  free_parsestate(pstate);

  return spec_list;
}

// Reference: RelationBuildPartitionKey
PartitionKey PaxRelationBuildPartitionKey(Relation relation,
                                          List *partparams_list) {
  int i;
  PartitionKey key;
  Oid *partopclass;
  ListCell *partexprs_item;
  int16 procnum;

  Assert(RelationIsPAX(relation));

  key = (PartitionKey)palloc0(sizeof(PartitionKeyData));
  key->strategy = PARTITION_STRATEGY_RANGE;
  key->partnatts = list_length(partparams_list);
  key->partattrs = (AttrNumber *)palloc(key->partnatts * sizeof(AttrNumber));
  key->partopfamily = (Oid *)palloc(key->partnatts * sizeof(Oid));
  key->partopcintype = (Oid *)palloc(key->partnatts * sizeof(Oid));
  key->partsupfunc = (FmgrInfo *)palloc0(key->partnatts * sizeof(FmgrInfo));

  key->partcollation = (Oid *)palloc(key->partnatts * sizeof(Oid));
  key->parttypid = (Oid *)palloc(key->partnatts * sizeof(Oid));
  key->parttypmod = (int32 *)palloc(key->partnatts * sizeof(int32));
  key->parttyplen = (int16 *)palloc(key->partnatts * sizeof(int16));
  key->parttypbyval = (bool *)palloc(key->partnatts * sizeof(bool));
  key->parttypalign = (char *)palloc(key->partnatts * sizeof(char));
  key->parttypcoll = (Oid *)palloc(key->partnatts * sizeof(Oid));

  partopclass = (Oid *)palloc(key->partnatts * sizeof(Oid));
  ComputePartitionAttrs(NULL, relation, partparams_list, key->partattrs, NULL,
                        partopclass, key->partcollation, key->strategy);

  /* determine support function number to search for */
  procnum = (key->strategy == PARTITION_STRATEGY_HASH) ? HASHEXTENDED_PROC
                                                       : BTORDER_PROC;

  // We don't have expressions as our partition keys, but keep
  // the code the same as the kernel.
  partexprs_item = list_head(key->partexprs);
  for (i = 0; i < key->partnatts; i++) {
    AttrNumber attno = key->partattrs[i];
    HeapTuple opclasstup;
    Form_pg_opclass opclassform;
    Oid funcid;

    /* Collect opfamily information */
    opclasstup = SearchSysCache1(CLAOID, ObjectIdGetDatum(partopclass[i]));
    if (!HeapTupleIsValid(opclasstup))
      elog(ERROR, "cache lookup failed for opclass %u", partopclass[i]);

    opclassform = (Form_pg_opclass)GETSTRUCT(opclasstup);
    key->partopfamily[i] = opclassform->opcfamily;
    key->partopcintype[i] = opclassform->opcintype;

    /* Get a support function for the specified opfamily and datatypes */
    funcid = get_opfamily_proc(opclassform->opcfamily, opclassform->opcintype,
                               opclassform->opcintype, procnum);
    if (!OidIsValid(funcid))
      ereport(
          ERROR,
          (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
           errmsg("operator class \"%s\" of access method %s is missing "
                  "support function %d for type %s",
                  NameStr(opclassform->opcname),
                  (key->strategy == PARTITION_STRATEGY_HASH) ? "hash" : "btree",
                  procnum, format_type_be(opclassform->opcintype))));

    fmgr_info_cxt(funcid, &key->partsupfunc[i], CurrentMemoryContext);

    /* Collect type information */
    if (attno != 0) {
      Form_pg_attribute att = TupleDescAttr(relation->rd_att, attno - 1);

      key->parttypid[i] = att->atttypid;
      key->parttypmod[i] = att->atttypmod;
      key->parttypcoll[i] = att->attcollation;
    } else {
      if (partexprs_item == NULL)
        elog(ERROR, "wrong number of partition key expressions");

      key->parttypid[i] = exprType(static_cast<Node *>(lfirst(partexprs_item)));
      key->parttypmod[i] =
          exprTypmod(static_cast<Node *>(lfirst(partexprs_item)));
      key->parttypcoll[i] =
          exprCollation(static_cast<Node *>(lfirst(partexprs_item)));

      partexprs_item = lnext(key->partexprs, partexprs_item);
    }
    get_typlenbyvalalign(key->parttypid[i], &key->parttyplen[i],
                         &key->parttypbyval[i], &key->parttypalign[i]);

    ReleaseSysCache(opclasstup);
  }
  pfree(partopclass);
  return key;
}

static PartitionDesc PaxRelationBuildPartitionDesc(PartitionKey key,
                                                   List *partboundspec_list,
                                                   MemoryContext tmp_ctx,
                                                   MemoryContext target_ctx) {
  PartitionDesc partdesc;
  PartitionBoundInfo boundinfo;
  PartitionBoundSpec **boundspecs = NULL;
  int nparts;
  MemoryContext saved_cxt;
  int *mapping;

  saved_cxt = MemoryContextSwitchTo(tmp_ctx);
  nparts = list_length(partboundspec_list);
  boundspecs =
      (PartitionBoundSpec **)palloc(nparts * sizeof(PartitionBoundSpec *));
  for (int i = 0; i < nparts; i++)
    boundspecs[i] =
        static_cast<PartitionBoundSpec *>(list_nth(partboundspec_list, i));

  /*
   * Create PartitionBoundInfo and mapping, working in the caller's context.
   * This could fail, but we haven't done any damage if so.
   */
  boundinfo = partition_bounds_create(boundspecs, nparts, key, &mapping);
  pfree(boundspecs);

  MemoryContextSwitchTo(target_ctx);
  partdesc = (PartitionDescData *)palloc0(sizeof(PartitionDescData));
  partdesc->nparts = nparts;
  partdesc->detached_exist = false;
  partdesc->boundinfo = partition_bounds_copy(boundinfo, key);
  pfree(boundinfo);

  // PAX doesn't have child partition tables
  partdesc->oids = NULL;
  partdesc->is_leaf = NULL;
  /* Return to caller's context, and blow away the temporary context. */
  MemoryContextSwitchTo(saved_cxt);
  return partdesc;
}

static void PaxFormPartitionKeyDatum(PartitionKey key, TupleTableSlot *slot,
                                     Datum *values, bool *isnull) {
  for (int i = 0; i < key->partnatts; i++) {
    AttrNumber keycol = key->partattrs[i];

    Assert(keycol > 0);
    values[i] = slot_getattr(slot, keycol, &isnull[i]);
  }
}

bool PartitionObjectInternal::Initialize(Relation pax_rel) {
  MemoryContext tmp_ctx;
  MemoryContext saved_ctx;
  List *partparams_list;
  List *partboundspec_list;
  PartitionKey key = NULL;
  PartitionDesc desc = NULL;
  bool ok;

  Assert(pax_rel);
  pax_rel_ = pax_rel;

  tmp_ctx = AllocSetContextCreate(CurrentMemoryContext, "tmp pax partition ctx",
                                  ALLOCSET_DEFAULT_SIZES);
  mctx_ = AllocSetContextCreate(CurrentMemoryContext, "pax partition ctx",
                                ALLOCSET_DEFAULT_SIZES);
  MemoryContextCopyAndSetIdentifier(mctx_, RelationGetRelationName(pax_rel));

  saved_ctx = MemoryContextSwitchTo(tmp_ctx);
  ok = PaxLoadPartitionSpec(RelationGetRelid(pax_rel), &partparams_list,
                            &partboundspec_list);
  if (!ok) goto out;

  MemoryContextSwitchTo(mctx_);

  // The partition keys have no strict constraint for DDLs.
  // The column names/types may be changed later by the user, but the PAX code
  // is not aware of it. So, we ignore these inconsistent changes for partition
  // writer.
  PG_TRY();
  {
    key = PaxRelationBuildPartitionKey(pax_rel, partparams_list);
    InitializeMergeInfo(key, partboundspec_list, tmp_ctx, mctx_);

    desc =
        PaxRelationBuildPartitionDesc(key, partboundspec_list, tmp_ctx, mctx_);
    partition_bound_spec_ = static_cast<List *>(copyObject(partboundspec_list));
  }
  PG_CATCH();
  {
    // fall back to not use the partition writer
    ok = false;
    FlushErrorState();
  }
  PG_END_TRY();
  partition_key_ = key;
  partition_desc_ = desc;
out:
  MemoryContextSwitchTo(saved_ctx);
  MemoryContextDelete(tmp_ctx);
  return ok;
}

void PartitionObjectInternal::InitializeMergeInfo(PartitionKey key,
                                                  List *partboundspec_list,
                                                  MemoryContext tmp_ctx,
                                                  MemoryContext target_ctx) {
  // gather whether the adjacent bounds are continuous
  // NOTE: the bounds are already sorted.
  MemoryContext saved_ctx;
  int *merge_index;
  int nparts;
  int merge_len;

  saved_ctx = MemoryContextSwitchTo(tmp_ctx);
  nparts = list_length(partboundspec_list);
  merge_index = (int *)palloc(2 * nparts * sizeof(int));
  merge_index[0] = 0;
  merge_len = 1;
  for (int i = 1; i < nparts; i++) {
    PartitionBoundSpec *spec1 =
        castNode(PartitionBoundSpec, list_nth(partboundspec_list, i - 1));
    PartitionBoundSpec *spec2 =
        castNode(PartitionBoundSpec, list_nth(partboundspec_list, i));

    auto cmpval = PartitionComparePartitionKeys(key, spec1->upperdatums,
                                                spec2->lowerdatums);
    Assert(cmpval <= 0);
    if (cmpval != 0) {
      merge_index[merge_len++] = i - 1;
      merge_index[merge_len++] = i;
    }
  }
  merge_index[merge_len++] = nparts - 1;

  Assert(merge_len % 2 == 0);
  MemoryContextSwitchTo(target_ctx);
  merge_len_ = merge_len;
  merge_index_ = (int *)palloc(merge_len * sizeof(int));
  memcpy(merge_index_, merge_index, merge_len * sizeof(int));
  pfree(merge_index);

  MemoryContextSwitchTo(saved_ctx);
}

void PartitionObjectInternal::Release() {
  pax_rel_ = nullptr;
  partition_key_ = nullptr;
  partition_desc_ = nullptr;
  partition_bound_spec_ = nullptr;
  if (mctx_) {
    MemoryContextDelete(mctx_);
    mctx_ = nullptr;
  }
}

int PartitionObjectInternal::NumPartitions() const {
  Assert(pax_rel_ && partition_key_ && partition_desc_ && mctx_);
  return list_length(partition_bound_spec_);
}

int PartitionObjectInternal::NumPartitionKeys() const {
  Assert(pax_rel_ && partition_key_ && partition_desc_ && mctx_);
  return get_partition_natts(partition_key_);
}

int PartitionObjectInternal::FindPartition(TupleTableSlot *slot) {
  Datum values[PARTITION_MAX_KEYS];
  bool isnull[PARTITION_MAX_KEYS];

  Assert(pax_rel_ && partition_key_ && partition_desc_ && mctx_);
  PaxFormPartitionKeyDatum(partition_key_, slot, values, isnull);
  return get_partition_for_tuple(partition_key_, partition_desc_, values,
                                 isnull);
}

}  // namespace paxc

namespace pax {
bool PartitionObject::Initialize(Relation pax_rel) {
  // FIXME: We MUST catch some types of exceptions and assumes
  // the partition should be ignored. Because the partition constraint
  // may be broken by:
  // 1. rename column name
  // 2. change column type
  // 3. drop one or more columns in the partition keys
  CBDB_WRAP_START;
  { return stub_.Initialize(pax_rel); }
  CBDB_WRAP_END;
}
void PartitionObject::Release() {
  CBDB_WRAP_START;
  { stub_.Release(); }
  CBDB_WRAP_END;
}

int PartitionObject::FindPartition(TupleTableSlot *slot) {
  CBDB_WRAP_START;
  { return stub_.FindPartition(slot); }
  CBDB_WRAP_END;
}
std::pair<int *, size_t> PartitionObject::GetMergeListInfo() {
  return {stub_.merge_index_, stub_.merge_len_};
}

}  // namespace pax

extern "C" {
// CREATE FUNCTION pax_dump_ranges(relid Oid) RETURNS SETOF TEXT AS
// '$libdir/pax', 'PaxPartitionDumpRanges'
// LANGUAGE C STRICT;
// UDF about partition
PG_FUNCTION_INFO_V1(PaxPartitionDumpRanges);
struct PartitionRangeDumpContext {
  List *boundspec_list;
  MemoryContext mctx;
  int index;
};

Datum PaxPartitionDumpRanges(PG_FUNCTION_ARGS) {
  PartitionRangeDumpContext *ctx;
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    Oid relid = PG_GETARG_OID(0);
    MemoryContext tmp_ctx;
    MemoryContext old_ctx;
    List *partparams;
    List *partboundspecs;
    bool ok;

    funcctx = SRF_FIRSTCALL_INIT();
    tmp_ctx =
        AllocSetContextCreate(funcctx->multi_call_memory_ctx,
                              "tmp pax partition ctx", ALLOCSET_DEFAULT_SIZES);
    old_ctx = MemoryContextSwitchTo(tmp_ctx);

    ok = paxc::PaxLoadPartitionSpec(relid, &partparams, &partboundspecs);
    if (!ok) partboundspecs = nullptr;

    ctx =
        (PartitionRangeDumpContext *)palloc(sizeof(PartitionRangeDumpContext));
    ctx->boundspec_list = partboundspecs;
    ctx->mctx = tmp_ctx;
    ctx->index = 0;
    funcctx->user_fctx = (void *)ctx;
    MemoryContextSwitchTo(old_ctx);
  }

  funcctx = SRF_PERCALL_SETUP();
  ctx = (PartitionRangeDumpContext *)funcctx->user_fctx;
  while (ctx->index < list_length(ctx->boundspec_list)) {
    StringInfoData str;
    char *value_list;
    text *range;
    PartitionBoundSpec *spec =
        castNode(PartitionBoundSpec, list_nth(ctx->boundspec_list, ctx->index));
    ++ctx->index;

    initStringInfo(&str);
    appendStringInfoString(&str, "from");
    value_list = get_range_partbound_string(spec->lowerdatums);
    appendStringInfoString(&str, value_list);
    pfree(value_list);

    appendStringInfoString(&str, " to");
    value_list = get_range_partbound_string(spec->upperdatums);
    appendStringInfoString(&str, value_list);
    pfree(value_list);

    range = cstring_to_text(str.data);
    pfree(str.data);
    SRF_RETURN_NEXT(funcctx, PointerGetDatum(range));
  }

  MemoryContextDelete(ctx->mctx);
  SRF_RETURN_DONE(funcctx);
}
}
