#include "access/paxc_rel_options.h"

#include "comm/cbdb_wrappers.h"

namespace paxc {

typedef struct {
  const char *optname; /* option's name */
  const pax::ColumnEncoding_Kind kind;
} relopt_compress_type_mapping;

static const relopt_compress_type_mapping kSelfRelCompressMap[] = {
    {ColumnEncoding_Kind_NO_ENCODED_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED},
    {ColumnEncoding_Kind_RLE_V2_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2},
    {ColumnEncoding_Kind_DIRECT_DELTA_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA},
    {ColumnEncoding_Kind_COMPRESS_ZSTD_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD},
    {ColumnEncoding_Kind_COMPRESS_ZLIB_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB},
};

typedef struct {
  const char *optname; /* option's name */
  const pax::PaxStorageFormat format;
} relopt_format_type_mapping;

static const relopt_format_type_mapping kSelfRelFormatMap[] = {
    {STORAGE_FORMAT_TYPE_PORC, pax::PaxStorageFormat::kTypeStoragePorcNonVec},
    {STORAGE_FORMAT_TYPE_PORC_VEC, pax::PaxStorageFormat::kTypeStoragePorcVec},
};

// reloptions structure and variables.
static relopt_kind self_relopt_kind;

#define PAX_COPY_STR_OPT(pax_opts_, pax_opt_name_)                            \
  do {                                                                        \
    PaxOptions *pax_opts = reinterpret_cast<PaxOptions *>(pax_opts_);         \
    int pax_name_offset_ = *reinterpret_cast<int *>(pax_opts->pax_opt_name_); \
    if (pax_name_offset_)                                                     \
      strlcpy(pax_opts->pax_opt_name_,                                        \
              reinterpret_cast<char *>(pax_opts) + pax_name_offset_,          \
              sizeof(pax_opts->pax_opt_name_));                               \
  } while (0)

static const char *kSelfColumnEncodingClauseWhiteList[] = {
    PAX_SOPT_COMPTYPE,
    PAX_SOPT_COMPLEVEL,
};

static const relopt_parse_elt kSelfReloptTab[] = {
    // no allow set with encoding
    {PAX_SOPT_STORAGE_FORMAT, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, storage_format)},
    // allow with encoding
    {PAX_SOPT_COMPTYPE, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, compress_type)},
    {PAX_SOPT_COMPLEVEL, RELOPT_TYPE_INT, offsetof(PaxOptions, compress_level)},
    {PAX_SOPT_PARTITION_BY, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, partition_by_offset)},
    {PAX_SOPT_PARTITION_RANGES, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, partition_ranges_offset)},
    {PAX_SOPT_MINMAX_COLUMNS, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, minmax_columns_offset)},
};

static void paxc_validate_rel_options_storage_format(const char *value) {
  size_t i;

  for (i = 0; i < lengthof(kSelfRelFormatMap); i++) {
    if (strcmp(value, kSelfRelFormatMap[i].optname) == 0) return;
  }
  ereport(ERROR, (errmsg("unsupported storage format: '%s'", value)));
}

static void paxc_validate_rel_options_compress_type(const char *value) {
  size_t i;

  for (i = 0; i < lengthof(kSelfRelCompressMap); i++) {
    if (strcmp(value, kSelfRelCompressMap[i].optname) == 0) return;
  }
  ereport(ERROR, (errmsg("unsupported compress type: '%s'", value)));
}

static void paxc_validate_rel_option(PaxOptions *options) {
  Assert(options);
  if (strcmp(ColumnEncoding_Kind_NO_ENCODED_STR, options->compress_type) == 0 ||
      strcmp(ColumnEncoding_Kind_RLE_V2_STR, options->compress_type) == 0 ||
      strcmp(ColumnEncoding_Kind_DIRECT_DELTA_STR, options->compress_type) ==
          0) {
    if (options->compress_level != 0) {
      ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("compresslevel=%d should setting is not work for "
                             "current encoding.",
                             options->compress_level)));
    }
  }
}

bytea *paxc_default_rel_options(Datum reloptions, char /*relkind*/,
                                bool validate) {
  Assert(self_relopt_kind != 0);
  bytea *rdopts = (bytea *)build_reloptions(
      reloptions, validate, self_relopt_kind, sizeof(PaxOptions),
      kSelfReloptTab, lengthof(kSelfReloptTab));

  PAX_COPY_STR_OPT(rdopts, storage_format);
  PAX_COPY_STR_OPT(rdopts, compress_type);
  return rdopts;
}

PaxOptions **paxc_relation_get_attribute_options(Relation rel) {
  Datum *dats;
  PaxOptions **opts;
  int i;

  Assert(rel && OidIsValid(RelationGetRelid(rel)));

  opts = (PaxOptions **)palloc0(RelationGetNumberOfAttributes(rel) *
                                sizeof(PaxOptions *));

  dats = get_rel_attoptions(RelationGetRelid(rel),
                            RelationGetNumberOfAttributes(rel));

  for (i = 0; i < RelationGetNumberOfAttributes(rel); i++) {
    if (DatumGetPointer(dats[i]) != NULL) {
      opts[i] = (PaxOptions *)paxc_default_rel_options(dats[i], 0, false);
      pfree(DatumGetPointer(dats[i]));
    }
  }
  pfree(dats);

  return opts;
}

static void paxc_validate_single_column_encoding_clauses(
    List *single_column_encoding) {
  ListCell *cell = NULL;
  Datum d;
  PaxOptions *option = NULL;
  /* not allow caller pass the `PAX_SOPT_STORAGE_FORMAT`
   */
  foreach (cell, single_column_encoding) {
    DefElem *def = (DefElem *)lfirst(cell);
    bool not_in_white_list = true;

    if (!def->defname) {
      continue;
    }

    for (size_t i = 0; i < lengthof(kSelfColumnEncodingClauseWhiteList); i++) {
      if (strcmp(kSelfColumnEncodingClauseWhiteList[i], def->defname) == 0) {
        not_in_white_list = false;
        break;
      }
    }

    if (not_in_white_list) {
      ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("%s not allow setting in ENCODING CLAUSES.",
                             def->defname)));
    }
  }

  d = transformRelOptions(PointerGetDatum(NULL), single_column_encoding, NULL,
                          NULL, true, false);

  option = (PaxOptions *)paxc_default_rel_options(d, 0, true);
  paxc_validate_rel_option(option);
}

void paxc_validate_column_encoding_clauses(List *encoding_opts) {
  ListCell *lc;
  foreach (lc, encoding_opts) {
    ColumnReferenceStorageDirective *crsd =
        (ColumnReferenceStorageDirective *)lfirst(lc);
    paxc_validate_single_column_encoding_clauses(crsd->encoding);
  }
}

List *paxc_transform_column_encoding_clauses(List *encoding_opts, bool validate,
                                             bool fromType) {
  List *ret_list = NIL;

  if (fromType) {
    return NIL;
  }

  ret_list = list_copy(encoding_opts);
  /* there are no need to do column encoding clauses transform in pax
   * because pax will setting default encoding inside
   */
  if (validate) {
    paxc_validate_single_column_encoding_clauses(encoding_opts);
  }

  /* if column no setting the encoding clauses
   * in transformColumnEncoding will pass the relation option
   * to column encoding clauses, should remove the
   * `PAX_SOPT_STORAGE_FORMAT` from it.
   */
  ListCell *cell = NULL;
  foreach (cell, ret_list) {
    DefElem *def = (DefElem *)lfirst(cell);
    bool not_in_white_list = true;
    if (!def->defname) {
      continue;
    }

    for (size_t i = 0; i < lengthof(kSelfColumnEncodingClauseWhiteList); i++) {
      if (strcmp(kSelfColumnEncodingClauseWhiteList[i], def->defname) == 0) {
        not_in_white_list = false;
        break;
      }
    }

    if (not_in_white_list) {
      ret_list = foreach_delete_current(ret_list, cell);
    }
  }

  return ret_list;
}

Bitmapset *paxc_get_minmax_columns_index(Relation rel, bool validate) {
  Assert(rel->rd_rel->relam == PAX_AM_OID);

  List *list = NIL;
  ListCell *lc;
  auto tupdesc = RelationGetDescr(rel);
  auto natts = RelationGetNumberOfAttributes(rel);
  Bitmapset *bms = nullptr;

  auto options = reinterpret_cast<paxc::PaxOptions *>(rel->rd_options);
  if (!options || !options->minmax_columns()) return nullptr;

  auto value = pstrdup(options->minmax_columns());
  if (!SplitDirectoriesString(value, ',', &list))
    elog(ERROR, "invalid minmax_columns: '%s' '%s'", value,
         options->minmax_columns());

  pfree(value);
  foreach (lc, list) {
    auto s = (char *)lfirst(lc);
    int i;
    bool dropped_column = false;
    bool valid_column;

    for (i = 0; i < natts; i++) {
      auto attr = TupleDescAttr(tupdesc, i);
      if (strcmp(s, NameStr(attr->attname)) == 0) {
        if (!attr->attisdropped) break;

        if (validate) elog(ERROR, "pax: can't use dropped column");
        dropped_column = true;
        break;
      }
    }
    valid_column = !dropped_column && i < natts;
    if (validate) {
      if (i == natts) elog(ERROR, "invalid column name '%s'", s);

      if (bms_is_member(i, bms)) elog(ERROR, "duplicated column name '%s'", s);
    }

    if (valid_column) bms = bms_add_member(bms, i);
  }
  list_free_deep(list);
  return bms;
}

void paxc_reg_rel_options() {
  self_relopt_kind = add_reloption_kind();
  add_string_reloption(
      self_relopt_kind, PAX_SOPT_STORAGE_FORMAT, "pax storage format", "porc",
      paxc_validate_rel_options_storage_format, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_COMPTYPE, "pax compress type",
                       PAX_DEFAULT_COMPRESSTYPE,
                       paxc_validate_rel_options_compress_type,
                       AccessExclusiveLock);
  add_int_reloption(self_relopt_kind, PAX_SOPT_COMPLEVEL, "pax compress level",
                    PAX_DEFAULT_COMPRESSLEVEL, PAX_MIN_COMPRESSLEVEL,
                    PAX_MAX_COMPRESSLEVEL, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_PARTITION_BY, "partition by",
                       NULL, NULL, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_PARTITION_RANGES,
                       "partition ranges", NULL, NULL, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_MINMAX_COLUMNS,
                       "minmax columns", NULL, NULL, AccessExclusiveLock);
}

}  // namespace paxc

namespace pax {

ColumnEncoding_Kind CompressKeyToColumnEncodingKind(const char *encoding_str) {
  Assert(encoding_str);

  for (size_t i = 0; i < lengthof(paxc::kSelfRelCompressMap); i++) {
    if (strcmp(paxc::kSelfRelCompressMap[i].optname, encoding_str) == 0) {
      return paxc::kSelfRelCompressMap[i].kind;
    }
  }

  CBDB_RAISE(cbdb::CException::kExTypeLogicError);
}

PaxStorageFormat StorageFormatKeyToPaxStorageFormat(
    const char *storage_format_str) {
  Assert(storage_format_str);

  for (size_t i = 0; i < lengthof(paxc::kSelfRelFormatMap); i++) {
    if (strcmp(paxc::kSelfRelFormatMap[i].optname, storage_format_str) == 0) {
      return paxc::kSelfRelFormatMap[i].format;
    }
  }

  CBDB_RAISE(cbdb::CException::kExTypeLogicError);
}

}  // namespace pax

std::vector<int> cbdb::GetMinMaxColumnsIndex(Relation rel) {
  std::vector<int> indexes;
  Bitmapset *bms;
  int idx;

  {
    CBDB_WRAP_START;
    { bms = paxc::paxc_get_minmax_columns_index(rel, false); }
    CBDB_WRAP_END;
  }

  idx = -1;
  while ((idx = bms_next_member(bms, idx)) >= 0) {
    indexes.push_back(idx);
  }

  {
    CBDB_WRAP_START;
    { bms_free(bms); }
    CBDB_WRAP_END;
  }

  return indexes;
}
