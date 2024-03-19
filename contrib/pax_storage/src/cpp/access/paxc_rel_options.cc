#include "access/paxc_rel_options.h"

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
    {STORAGE_FORMAT_TYPE_ORC, pax::PaxStorageFormat::kTypeStorageOrcNonVec},
    {STORAGE_FORMAT_TYPE_ORC_VEC, pax::PaxStorageFormat::kTypeStorageOrcVec},
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
    {PAX_SOPT_NUMERIC_VEC_STORAGE, RELOPT_TYPE_BOOL,
     offsetof(PaxOptions, numeric_vec_storage)},
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

void paxc_reg_rel_options() {
  self_relopt_kind = add_reloption_kind();
  add_string_reloption(
      self_relopt_kind, PAX_SOPT_STORAGE_FORMAT, "pax storage format", "orc",
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
  add_bool_reloption(self_relopt_kind, PAX_SOPT_NUMERIC_VEC_STORAGE,
                     "use vec format store numeric type", false,
                     AccessExclusiveLock);
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
