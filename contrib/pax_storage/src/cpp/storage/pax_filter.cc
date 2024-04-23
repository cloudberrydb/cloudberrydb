#include "storage/pax_filter.h"

#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "storage/micro_partition_stats.h"
#include "storage/proto/proto_wrappers.h"

namespace paxc {

static bool BuildScanKeyOpExpr(ScanKey this_scan_key, Expr *clause,
                               const Oid *opfamilies, bool isorderby,
                               int indnkeyatts);
static bool BuildScanKeyNullTest(ScanKey this_scan_key, Expr *clause);

static bool BuildScanKeyOpExpr(ScanKey this_scan_key, Expr *clause,
                               const Oid *opfamilies, bool isorderby,
                               int indnkeyatts) {
  /* indexkey op const or indexkey op expression */
  int flags = 0;
  Datum scanvalue;

  Oid opno;              /* operator's OID */
  RegProcedure opfuncid; /* operator proc id used in scan */
  Oid opfamily;          /* opfamily of index column */
  int op_strategy;       /* operator's strategy number */
  Oid op_lefttype;       /* operator's declared input types */
  Oid op_righttype;
  Expr *leftop;        /* expr on lhs of operator */
  Expr *rightop;       /* expr on rhs ... */
  AttrNumber varattno; /* att number used in scan */

  opno = ((OpExpr *)clause)->opno;
  opfuncid = ((OpExpr *)clause)->opfuncid;

  /*
   * leftop should be the index key Var, possibly relabeled
   */
  leftop = (Expr *)get_leftop(clause);

  if (leftop && IsA(leftop, RelabelType)) leftop = ((RelabelType *)leftop)->arg;

  Assert(leftop != NULL);

  if (!IsA(leftop, Var)) goto ignore_clause;

  varattno = ((Var *)leftop)->varattno;
  if (varattno < 1 || varattno > indnkeyatts) goto ignore_clause;

  /*
   * We have to look up the operator's strategy number.  This
   * provides a cross-check that the operator does match the index.
   */
  opfamily = opfamilies[varattno - 1];
  if (!OidIsValid(opfamily) || !op_in_opfamily(opno, opfamily))
    goto ignore_clause;

  get_op_opfamily_properties(opno, opfamily, isorderby, &op_strategy,
                             &op_lefttype, &op_righttype);

  if (isorderby) flags |= SK_ORDER_BY;

  /*
   * rightop is the constant or variable comparison value
   */
  rightop = (Expr *)get_rightop(clause);

  if (rightop && IsA(rightop, RelabelType))
    rightop = ((RelabelType *)rightop)->arg;

  Assert(rightop != NULL);

  if (IsA(rightop, Const)) {
    /* OK, simple constant comparison value */
    scanvalue = ((Const *)rightop)->constvalue;
    if (((Const *)rightop)->constisnull) flags |= SK_ISNULL;
  } else {
    // No support for runtime keys now
    goto ignore_clause;
  }

  /*
   * initialize the scan key's fields appropriately
   */
  ScanKeyEntryInitialize(this_scan_key, flags,
                         varattno,     /* attribute number to scan */
                         op_strategy,  /* op's strategy */
                         op_righttype, /* strategy subtype */
                         ((OpExpr *)clause)->inputcollid, /* collation */
                         opfuncid,                        /* reg proc to use */
                         scanvalue);                      /* constant */
  return true;
ignore_clause:
  return false;
}

static bool BuildScanKeyNullTest(ScanKey this_scan_key, Expr *clause) {
  auto ntest = reinterpret_cast<NullTest *>(clause);
  int flags;

  Expr *leftop;        /* expr on lhs of operator */
  AttrNumber varattno; /* att number used in scan */
  /*
   * argument should be the index key Var, possibly relabeled
   */
  leftop = ntest->arg;

  if (leftop && IsA(leftop, RelabelType)) leftop = ((RelabelType *)leftop)->arg;

  Assert(leftop != NULL);

  if (!IsA(leftop, Var)) goto ignore_clause;

  varattno = ((Var *)leftop)->varattno;

  /*
   * initialize the scan key's fields appropriately
   */
  switch (ntest->nulltesttype) {
    case IS_NULL:
      flags = SK_ISNULL | SK_SEARCHNULL;
      break;
    case IS_NOT_NULL:
      flags = SK_ISNULL | SK_SEARCHNOTNULL;
      break;
    default:
      elog(ERROR, "unrecognized nulltesttype: %d", (int)ntest->nulltesttype);
      flags = 0; /* keep compiler quiet */
      break;
  }

  ScanKeyEntryInitialize(this_scan_key, flags,
                         varattno,        /* attribute number to scan */
                         InvalidStrategy, /* no strategy */
                         InvalidOid,      /* no strategy subtype */
                         InvalidOid,      /* no collation */
                         InvalidOid,      /* no reg proc for this */
                         (Datum)0);       /* constant */

  return true;
ignore_clause:
  return false;
}

static bool BuildScanKeys(Relation rel, List *quals, bool isorderby,
                          ScanKey *p_scan_keys, int *p_num_scan_keys) {
  List *flat_quals = NIL;
  ListCell *qual_cell, *lc1;
  ScanKey scan_keys;
  int n_scan_keys;
  int index_of_scan_key;
  TupleDesc desc;

  foreach (qual_cell, quals) {
    Expr *clause = (Expr *)lfirst(qual_cell);

    if (IsA(clause, BoolExpr)) {
      if (is_andclause(clause)) {
        foreach (lc1, ((BoolExpr *)clause)->args) {
          Expr *sub_clause = (Expr *)lfirst(lc1);
          flat_quals = lappend(flat_quals, sub_clause);
        }
      } else if (is_orclause(clause)) {
        // TODO: support it
      }
    } else {
      flat_quals = lappend(flat_quals, clause);
    }
  }

  /* Allocate array for ScanKey structs: one per qual */
  n_scan_keys = list_length(flat_quals);
  scan_keys = (ScanKey)palloc(n_scan_keys * sizeof(ScanKeyData));
  desc = rel->rd_att;
  Oid *opfamilies = (Oid *)palloc(sizeof(Oid) * desc->natts);
  for (auto i = 0; i < desc->natts; i++) {
    auto attr = &desc->attrs[i];
    if (attr->attisdropped) {
      opfamilies[i] = 0;
      continue;
    }
    Oid opclass = GetDefaultOpClass(attr->atttypid, BRIN_AM_OID);
    if (!OidIsValid(opclass)) {
      opfamilies[i] = 0;
      continue;
    }
    opfamilies[i] = get_opclass_family(opclass);
  }

  index_of_scan_key = 0;
  foreach (qual_cell, flat_quals) {
    Expr *clause = (Expr *)lfirst(qual_cell);
    ScanKey this_scan_key = &scan_keys[index_of_scan_key];
    int indnkeyatts;

    indnkeyatts = RelationGetNumberOfAttributes(rel);

    if (IsA(clause, OpExpr)) {
      /* indexkey op const or indexkey op expression */
      if (BuildScanKeyOpExpr(this_scan_key, clause, opfamilies, isorderby,
                             indnkeyatts)) {
        index_of_scan_key++;
      }
    } else if (IsA(clause, NullTest)) {
      /* indexkey IS NULL or indexkey IS NOT NULL */
      Assert(!isorderby);
      if (BuildScanKeyNullTest(this_scan_key, clause)) {
        index_of_scan_key++;
      }
    } else {
      // not support other qual types yet
    }
  }
  pfree(opfamilies);
  list_free(flat_quals);

  /*
   * Return info to our caller.
   */
  if (index_of_scan_key > 0) {
    *p_scan_keys = scan_keys;
    *p_num_scan_keys = index_of_scan_key;
    return true;
  }
  return false;
}

static inline void FindAttrsInQual(Node *qual, bool *proj, int ncol,
                                   int *proj_atts, int *num_proj_atts) {
  int i, k;
  /* get attrs in qual */
  extractcolumns_from_node(qual, proj, ncol);

  /* collect the number of proj attr and attr_no from proj[] */
  k = 0;
  for (i = 0; i < ncol; i++) {
    if (proj[i]) proj_atts[k++] = i;
  }
  *num_proj_atts = k;
}

bool BuildExecutionFilterForColumns(Relation rel, PlanState *ps,
                                    pax::ExecutionFilterContext *ctx) {
  List *qual = ps->plan->qual;
  List **qual_list;
  ListCell *lc;
  bool *proj;
  int *qual_atts;
  int natts = RelationGetNumberOfAttributes(rel);

  if (!qual || !IsA(qual, List)) return false;

  if (list_length(qual) == 1 && IsA(linitial(qual), BoolExpr)) {
    auto boolexpr = (BoolExpr *)linitial(qual);
    if (boolexpr->boolop != AND_EXPR) return false;
    qual = boolexpr->args;
  }
  Assert(IsA(qual, List));

  proj = (bool *)palloc(sizeof(bool) * natts);
  qual_atts = (int *)palloc(sizeof(int) * natts);
  qual_list = (List **)palloc0(sizeof(List *) * (natts + 1));

  ctx->econtext = ps->ps_ExprContext;
  ctx->estate_final = nullptr;
  ctx->estates = nullptr;
  ctx->attnos = nullptr;
  ctx->size = 0;

  foreach (lc, qual) {
    Expr *subexpr = (Expr *)lfirst(lc);
    int num_qual_atts = 0;
    int attno;

    Assert(subexpr);
    memset(proj, 0, sizeof(bool) * natts);
    FindAttrsInQual((Node *)subexpr, proj, natts, qual_atts, &num_qual_atts);
    if (num_qual_atts == 0 || num_qual_atts > 1) {
      qual_list[0] = lappend(qual_list[0], subexpr);
      continue;
    }
    attno = qual_atts[0] + 1;
    Assert(num_qual_atts == 1 && attno > 0 && attno <= natts);
    if (!qual_list[attno]) ctx->size++;
    qual_list[attno] = lappend(qual_list[attno], subexpr);
  }

  if (ctx->size > 0) {
    int k = 0;
    ctx->estates = (ExprState **)palloc(sizeof(ExprState *) * ctx->size);
    ctx->attnos = (AttrNumber *)palloc(sizeof(AttrNumber) * ctx->size);
    for (AttrNumber i = 1; i <= (AttrNumber)natts; i++) {
      if (!qual_list[i]) continue;
      ctx->estates[k] = ExecInitQual(qual_list[i], ps);
      ctx->attnos[k] = i;
      list_free(qual_list[i]);
      k++;
    }
    Assert(ctx->size == k);
  }
  if (qual_list[0]) {
    ctx->estate_final = ExecInitQual(qual_list[0], ps);
    list_free(qual_list[0]);
  }

  Assert(ctx->size > 0 || ctx->estate_final);
  ps->qual = nullptr;

  pfree(proj);
  pfree(qual_atts);
  pfree(qual_list);
  return true;
}

}  // namespace paxc

namespace pax {

bool BuildScanKeys(Relation rel, List *quals, bool isorderby,
                   ScanKey *scan_keys, int *num_scan_keys) {
  CBDB_WRAP_START;
  {
    return paxc::BuildScanKeys(rel, quals, isorderby, scan_keys, num_scan_keys);
  }
  CBDB_WRAP_END;
}

bool BuildExecutionFilterForColumns(Relation rel, PlanState *ps,
                                    pax::ExecutionFilterContext *ctx) {
  CBDB_WRAP_START;
  { return paxc::BuildExecutionFilterForColumns(rel, ps, ctx); }
  CBDB_WRAP_END;
}

PaxFilter::PaxFilter(bool allow_fallback_to_pg)
    : allow_fallback_to_pg_(allow_fallback_to_pg) {}

PaxFilter::~PaxFilter() { PAX_DELETE_ARRAY(proj_); }

std::pair<bool *, size_t> PaxFilter::GetColumnProjection() {
  return std::make_pair(proj_, proj_len_);
}

static bool ProjShouldReadAll(const bool *const proj_map, size_t proj_len) {
  if (!proj_map) {
    return true;
  }

  for (size_t i = 0; i < proj_len; i++) {
    if (!proj_map[i]) {
      return false;
    }
  }
  return true;
}

void PaxFilter::SetColumnProjection(bool *proj, size_t proj_len) {
  if (ProjShouldReadAll(proj, proj_len)) {
    proj_ = nullptr;
    proj_len_ = 0;
    proj_column_index_.clear();
  } else {
    proj_ = proj;
    proj_len_ = proj_len;
    proj_column_index_.clear();
    for (size_t i = 0; i < proj_len_; i++) {
      if (proj_[i]) proj_column_index_.emplace_back(i);
    }
  }
}

void PaxFilter::SetScanKeys(ScanKey scan_keys, int num_scan_keys) {
  Assert(num_scan_keys_ == 0);

  if (num_scan_keys > 0) {
    scan_keys_ = scan_keys;
    num_scan_keys_ = num_scan_keys;
  }
}

static inline bool CheckNullKey(ScanKey scan_key, bool allnull, bool hasnull) {
  // handle null test
  // SK_SEARCHNULL and SK_SEARCHNOTNULL must not co-exist with each other
  Assert(scan_key->sk_flags & SK_ISNULL);
  Assert((scan_key->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) !=
         (SK_SEARCHNULL | SK_SEARCHNOTNULL));

  if (scan_key->sk_flags & SK_SEARCHNULL) {
    // test: IS NULL
    if (!hasnull) return false;
  } else if (scan_key->sk_flags & SK_SEARCHNOTNULL) {
    // test: IS NOT NULL
    if (allnull) return false;
  } else {
    // Neither IS NULL nor IS NOT NULL was used; assume all indexable
    // operators are strict and thus return false with NULL value in
    // the scan key.
    return false;
  }
  return true;
}

static inline bool CheckOpfamily(const ::pax::stats::ColumnBasicInfo &info,
                                 Oid opfamily) {
  return info.opfamily() == opfamily;
}

static bool CheckNonnullValue(const ::pax::stats::ColumnBasicInfo &minmax,
                              const ::pax::stats::ColumnDataStats &data_stats,
                              ScanKey scan_key, Form_pg_attribute attr,
                              bool allow_fallback_to_pg) {
  Oid opfamily;
  FmgrInfo finfo;
  Datum datum;
  Datum matches = true;

  Assert(data_stats.has_minimal() == data_stats.has_maximum());
  if (!data_stats.has_minimal()) return true;

  auto value = scan_key->sk_argument;
  auto typid = attr->atttypid;
  auto collation = minmax.collation();
  auto typlen = attr->attlen;
  auto typbyval = attr->attbyval;

  bool ok;
  OperMinMaxFunc lfunc, lfunc2;

  switch (scan_key->sk_strategy) {
    case BTLessStrategyNumber:
    case BTLessEqualStrategyNumber: {
      if (MinMaxGetStrategyProcinfo(typid, scan_key->sk_subtype, collation,
                                    lfunc, scan_key->sk_strategy)) {
        Assert(lfunc);
        datum = pax::MicroPartitionStats::FromValue(data_stats.minimal(),
                                                    typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);

        matches = lfunc(&datum, &value, collation);
      } else if (allow_fallback_to_pg) {
        ok = cbdb::MinMaxGetStrategyProcinfo(typid, scan_key->sk_subtype,
                                             &opfamily, &finfo,
                                             scan_key->sk_strategy);
        if (!ok || !CheckOpfamily(minmax, opfamily)) break;
        datum = pax::MicroPartitionStats::FromValue(data_stats.minimal(),
                                                    typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
        matches = cbdb::FunctionCall2Coll(&finfo, collation, datum, value);
      }

      break;
    }
    case BTEqualStrategyNumber: {
      if (MinMaxGetStrategyProcinfo(typid, scan_key->sk_subtype, collation,
                                    lfunc, BTLessEqualStrategyNumber) &&
          MinMaxGetStrategyProcinfo(typid, scan_key->sk_subtype, collation,
                                    lfunc2, BTGreaterEqualStrategyNumber)) {
        Assert(lfunc && lfunc2);

        datum = pax::MicroPartitionStats::FromValue(data_stats.minimal(),
                                                    typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
        matches = lfunc(&datum, &value, collation);
        if (!DatumGetBool(matches))
          // not (min <= value) --> min > value
          break;

        datum = pax::MicroPartitionStats::FromValue(data_stats.maximum(),
                                                    typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
        matches = lfunc2(&datum, &value, collation);

      } else if (allow_fallback_to_pg) {
        ok = cbdb::MinMaxGetStrategyProcinfo(typid, scan_key->sk_subtype,
                                             &opfamily, &finfo,
                                             BTLessEqualStrategyNumber);
        if (!ok || !CheckOpfamily(minmax, opfamily)) break;
        datum = pax::MicroPartitionStats::FromValue(data_stats.minimal(),
                                                    typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
        matches = cbdb::FunctionCall2Coll(&finfo, collation, datum, value);

        if (!DatumGetBool(matches))
          // not (min <= value) --> min > value
          break;

        ok = cbdb::MinMaxGetStrategyProcinfo(typid, scan_key->sk_subtype,
                                             &opfamily, &finfo,
                                             BTGreaterEqualStrategyNumber);
        if (!ok || !CheckOpfamily(minmax, opfamily)) break;
        datum = pax::MicroPartitionStats::FromValue(data_stats.maximum(),
                                                    typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
        matches = cbdb::FunctionCall2Coll(&finfo, collation, datum, value);
      }
      break;
    }
    case BTGreaterEqualStrategyNumber:
    case BTGreaterStrategyNumber: {
      if (MinMaxGetStrategyProcinfo(typid, scan_key->sk_subtype, collation,
                                    lfunc, scan_key->sk_strategy)) {
        Assert(lfunc);
        datum = pax::MicroPartitionStats::FromValue(data_stats.maximum(),
                                                    typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);

        matches = lfunc(&datum, &value, collation);
      } else if (allow_fallback_to_pg) {
        ok = cbdb::MinMaxGetStrategyProcinfo(typid, scan_key->sk_subtype,
                                             &opfamily, &finfo,
                                             scan_key->sk_strategy);
        if (!ok || !CheckOpfamily(minmax, opfamily)) break;

        datum = pax::MicroPartitionStats::FromValue(data_stats.maximum(),
                                                    typlen, typbyval, &ok);
        CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError);
        matches = cbdb::FunctionCall2Coll(&finfo, collation, datum, value);
      }

      break;
    }
    default:
      // not support others `sk_strategy`
      matches = BoolGetDatum(true);
      break;
  }

  return DatumGetBool(matches);
}

static bool CheckNullKeys(const TupleDesc desc, ScanKey scan_key,
                          const ColumnStatsProvider &provider) {
  auto attno = scan_key->sk_attno;
  if (attno > 0) {
    Assert(!TupleDescAttr(desc, attno - 1)->attisdropped);
    // current column missing values
    // caused by add new column
    if ((attno - 1) >= provider.ColumnSize()) {
      return true;
    }
    return CheckNullKey(scan_key, provider.AllNull(attno - 1),
                        provider.HasNull(attno - 1));
  }

  // check all columns, see ExecEvalRowNullInt()
  Assert(attno == 0);

  // missing values in the columns, can't filter
  if (desc->natts != provider.ColumnSize()) return true;

  for (int i = 0; i < desc->natts; i++) {
    if (TupleDescAttr(desc, i)->attisdropped) continue;
    if (!CheckNullKey(scan_key, provider.AllNull(i), provider.HasNull(i)))
      return false;
  }
  return true;
}

// returns true: if the micro partition needs to scan
// returns false: the micro partition could be ignored
bool PaxFilter::TestScanInternal(const ColumnStatsProvider &provider,
                                 const TupleDesc desc) const {
  auto natts = desc->natts;
  auto column_stats_size = provider.ColumnSize();

  Assert(num_scan_keys_ > 0);
  Assert(column_stats_size <= natts);
  for (int i = 0; i < num_scan_keys_; i++) {
    auto scan_key = &scan_keys_[i];

    // Only Null test support sk_attno = 0.
    if (scan_key->sk_flags & SK_ISNULL) {
      if (!CheckNullKeys(desc, scan_key, provider)) return false;

      continue;
    }

    auto column_index = scan_key->sk_attno - 1;
    Assert(column_index >= 0 && column_index < natts);

    auto attr = &desc->attrs[column_index];
    // scan key should never contain dropped column
    Assert(!attr->attisdropped);
    // the collation in catalog and scan key should be consistent
    if (scan_key->sk_collation != attr->attcollation) return true;

    if (column_index >= column_stats_size)
      continue;  // missing attributes have no stats

    const auto &info = provider.ColumnInfo(column_index);
    const auto &data_stats = provider.DataStats(column_index);

    Assert(data_stats.has_minimal() == data_stats.has_maximum());

    // Check whether alter column type will result rewriting whole table.
    AssertImply(info.typid(), attr->atttypid == info.typid());

    if (provider.AllNull(column_index)) {
      // ALL values are null, but the scan key is not null
      return false;
    } else if (scan_key->sk_collation != info.collation()) {
      // collation doesn't match ignore this scan key
    } else if (!CheckNonnullValue(info, data_stats, scan_key, attr,
                                  allow_fallback_to_pg_)) {
      return false;
    }
  }
  return true;
}

void PaxFilter::FillRemainingColumns(Relation rel) {
  int natts = RelationGetNumberOfAttributes(rel);
  bool *atts = PAX_NEW_ARRAY<bool>(natts);
  if (proj_len_ > 0) {
    Assert(natts >= 0 && static_cast<size_t>(natts) >= proj_len_);
    memcpy(atts, proj_, sizeof(bool) * proj_len_);
    for (auto i = static_cast<int>(proj_len_); i < natts; i++) atts[i] = false;
  } else {
    for (int i = 0; i < natts; i++) atts[i] = true;
  }
  // minus attnos in efctx_.attnos
  for (int i = 0; i < efctx_.size; i++) {
    auto attno = efctx_.attnos[i];
    Assert(attno > 0 && attno <= natts);
    atts[attno - 1] = false;
  }
  for (AttrNumber attno = 1; attno <= (AttrNumber)natts; attno++) {
    if (atts[attno - 1]) remaining_attnos_.emplace_back(attno);
  }
  PAX_DELETE_ARRAY(atts);
}

}  // namespace pax
