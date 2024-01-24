#include "storage/oper/pax_oper.h"

#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"

namespace pax {

namespace boolop {

// oper(bool, bool)
bool BoolLT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) < *((const bool *)r));
}

bool BoolLE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) <= *((const bool *)r));
}

bool BoolEQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) == *((const bool *)r));
}

bool BoolGE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) >= *((const bool *)r));
}

bool BoolGT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) > *((const bool *)r));
}

}  // namespace boolop

namespace charop {

// oper(char, char)
bool CharLT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) < *((const uint8 *)r));
}

bool CharLE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) <= *((const uint8 *)r));
}

bool CharEQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) == *((const uint8 *)r));
}

bool CharGE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) >= *((const uint8 *)r));
}

bool CharGT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) > *((const uint8 *)r));
}

}  // namespace charop

namespace int2op {

// oper(int2, int2)
bool Int2LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) < *((const int16 *)r));
}

bool Int2LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) <= *((const int16 *)r));
}

bool Int2EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) == *((const int16 *)r));
}

bool Int2GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) >= *((const int16 *)r));
}

bool Int2GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) > *((const int16 *)r));
}

// oper(int2, int4)
bool Int24LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) < *((const int32 *)r));
}

bool Int24LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) <= *((const int32 *)r));
}

bool Int24EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) == *((const int32 *)r));
}

bool Int24GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) >= *((const int32 *)r));
}

bool Int24GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) > *((const int32 *)r));
}

// oper(int2, int8)
bool Int28LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) < *((const int64 *)r));
}

bool Int28LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) <= *((const int64 *)r));
}

bool Int28EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) == *((const int64 *)r));
}

bool Int28GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) >= *((const int64 *)r));
}

bool Int28GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) > *((const int64 *)r));
}

}  // namespace int2op

namespace dateop {

// oper(date, date)
bool DateLT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) < *((const DateADT *)r));
}

bool DateLE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) <= *((const DateADT *)r));
}

bool DateEQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) == *((const DateADT *)r));
}

bool DateGE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) >= *((const DateADT *)r));
}

bool DateGT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) > *((const DateADT *)r));
}

}  // namespace dateop

namespace int4op {

// oper(int4, int4)
bool Int4LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) < *((const int32 *)r));
}

bool Int4LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) <= *((const int32 *)r));
}

bool Int4EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) == *((const int32 *)r));
}

bool Int4GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) >= *((const int32 *)r));
}

bool Int4GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) > *((const int32 *)r));
}

// oper(int4, int8)
bool Int48LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) < *((const int64 *)r));
}

bool Int48LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) <= *((const int64 *)r));
}

bool Int48EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) == *((const int64 *)r));
}

bool Int48GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) >= *((const int64 *)r));
}

bool Int48GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) > *((const int64 *)r));
}

// oper(int4, int2)
bool Int42LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) < *((const int16 *)r));
}

bool Int42LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) <= *((const int16 *)r));
}

bool Int42EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) == *((const int16 *)r));
}

bool Int42GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) >= *((const int16 *)r));
}

bool Int42GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) > *((const int16 *)r));
}

}  // namespace int4op

namespace int8op {

// oper(int8, int8)
bool Int8LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) < *((const int64 *)r));
}

bool Int8LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) <= *((const int64 *)r));
}

bool Int8EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) == *((const int64 *)r));
}

bool Int8GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) >= *((const int64 *)r));
}

bool Int8GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) > *((const int64 *)r));
}

// oper(int8, int4)
bool Int84LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) < *((const int32 *)r));
}

bool Int84LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) <= *((const int32 *)r));
}

bool Int84EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) == *((const int32 *)r));
}

bool Int84GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) >= *((const int32 *)r));
}

bool Int84GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) > *((const int32 *)r));
}

// oper(int8, int2)
bool Int82LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) < *((const int16 *)r));
}

bool Int82LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) <= *((const int16 *)r));
}

bool Int82EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) == *((const int16 *)r));
}

bool Int82GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) >= *((const int16 *)r));
}

bool Int82GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) > *((const int16 *)r));
}

}  // namespace int8op

namespace float4op {

bool Float4LT(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 < val2);
}

bool Float4LE(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 <= val2);
}

bool Float4EQ(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

bool Float4GE(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 >= val2);
}

bool Float4GT(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 > val2);
}

bool Float48LT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 < val2);
}

bool Float48LE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 <= val2);
}

bool Float48EQ(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

bool Float48GE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 >= val2);
}

bool Float48GT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 > val2);
}

}  // namespace float4op

namespace float8op {

bool Float8LT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 < val2);
}

bool Float8LE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 <= val2);
}

bool Float8EQ(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

bool Float8GE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 >= val2);
}

bool Float8GT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 > val2);
}

bool Float84LT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 < val2);
}

bool Float84LE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 <= val2);
}

bool Float84EQ(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

bool Float84GE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 >= val2);
}

bool Float84GT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 > val2);
}

}  // namespace float8op

namespace textop {
#define TEXTBUFLEN 1024

static inline bool LocaleIsC(Oid collation) {
  static int result = -1;
  if (result != -1) {
    return (bool)result;
  }

  /*
   * If we're asked about the default collation, we have to inquire of the C
   * library.  Cache the result so we only have to compute it once.
   */

  if (collation == DEFAULT_COLLATION_OID) {
    char *localeptr;
    localeptr = setlocale(LC_COLLATE, NULL);
    CBDB_CHECK(localeptr, cbdb::CException::ExType::kExTypeCError);

    if ((strcmp(localeptr, "C") == 0 || strcmp(localeptr, "POSIX") == 0)) {
      result = 1;
    } else {
      result = 0;
    }

  } else if (collation == C_COLLATION_OID || collation == POSIX_COLLATION_OID) {
    result = 1;
  } else {
    result = 0;
  }

  return (bool)result;
}

static inline int VarstrCmp(const char *arg1, int len1, const char *arg2,
                            int len2, Oid collid) {
  int rc;

  CBDB_CHECK(OidIsValid(collid), cbdb::CException::ExType::kExTypeLogicError);
  if (LocaleIsC(collid)) {
    rc = memcmp(arg1, arg2, Min(len1, len2));
    if ((rc == 0) && (len1 != len2)) rc = (len1 < len2) ? -1 : 1;
  } else if (collid == DEFAULT_COLLATION_OID) {
    char a1buf[TEXTBUFLEN];
    char a2buf[TEXTBUFLEN];
    char *a1p, *a2p;
    if (len1 == len2 && memcmp(arg1, arg2, len1) == 0) return 0;

    a1p = (len1 >= TEXTBUFLEN) ? (char *)cbdb::Palloc(len1 + 1) : a1buf;
    a2p = (len2 >= TEXTBUFLEN) ? (char *)cbdb::Palloc(len2 + 1) : a2buf;

    memcpy(a1p, arg1, len1);
    a1p[len1] = '\0';
    memcpy(a2p, arg2, len2);
    a2p[len2] = '\0';

    rc = strcoll(a1p, a2p);

    if (rc == 0) rc = strcmp(a1p, a2p);

    if (a1p != a1buf) cbdb::Pfree(a1p);

    if (a2p != a2buf) cbdb::Pfree(a2p);
  } else {
    // not support special provider
    CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
  }

  return rc;
}

static inline int TextCmp(const text *arg1, const text *arg2, Oid collid) {
  // safe to direct call toast_raw_datum_size
  return VarstrCmp(VARDATA_ANY(arg1), VARSIZE_ANY_EXHDR(arg1),
                   VARDATA_ANY(arg2), VARSIZE_ANY_EXHDR(arg2), collid);
}

bool TextLT(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) < 0;
}

bool TextLE(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) <= 0;
}

bool TextEQ(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) == 0;
}

bool TextGE(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) >= 0;
}

bool TextGT(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) > 0;
}

}  // namespace textop

namespace numericop {

bool NumericLT(const void *l, const void *r, Oid collation) {
  // safe to direct call pg function
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) < 0;
}

bool NumericLE(const void *l, const void *r, Oid collation) {
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) <= 0;
}

bool NumericEQ(const void *l, const void *r, Oid collation) {
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) == 0;
}

bool NumericGE(const void *l, const void *r, Oid collation) {
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) >= 0;
}

bool NumericGT(const void *l, const void *r, Oid collation) {
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) > 0;
}

}  // namespace numericop

#define INIT_MIN_MAX_OPER(L_OID, R_OID, SN, FUNC) \
  { {L_OID, R_OID, SN}, FUNC }

std::map<OperMinMaxKey, OperMinMaxFunc> min_max_opers = {
    // oper(bool, bool)
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTLessStrategyNumber, boolop::BoolLT),
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTLessEqualStrategyNumber,
                      boolop::BoolLE),
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTEqualStrategyNumber, boolop::BoolEQ),
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTGreaterEqualStrategyNumber,
                      boolop::BoolGE),
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTGreaterStrategyNumber,
                      boolop::BoolGT),

    // oper(char, char)
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTLessStrategyNumber, charop::CharLT),
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTLessEqualStrategyNumber,
                      charop::CharLE),
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTEqualStrategyNumber, charop::CharEQ),
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTGreaterEqualStrategyNumber,
                      charop::CharGE),
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTGreaterStrategyNumber,
                      charop::CharGT),

    // oper(int2, int2)
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTLessStrategyNumber, int2op::Int2LT),
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTLessEqualStrategyNumber,
                      int2op::Int2LE),
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTEqualStrategyNumber, int2op::Int2EQ),
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTGreaterEqualStrategyNumber,
                      int2op::Int2GE),
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTGreaterStrategyNumber,
                      int2op::Int2GT),

    // oper(int2, int4)
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTLessStrategyNumber, int2op::Int24LT),
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTLessEqualStrategyNumber,
                      int2op::Int24LE),
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTEqualStrategyNumber, int2op::Int24EQ),
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTGreaterEqualStrategyNumber,
                      int2op::Int24GE),
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTGreaterStrategyNumber,
                      int2op::Int24GT),

    // oper(int2, int8)
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTLessStrategyNumber, int2op::Int28LT),
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTLessEqualStrategyNumber,
                      int2op::Int28LE),
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTEqualStrategyNumber, int2op::Int28EQ),
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTGreaterEqualStrategyNumber,
                      int2op::Int28GE),
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTGreaterStrategyNumber,
                      int2op::Int28GT),

    // oper(date, date)
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTLessStrategyNumber, dateop::DateLT),
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTLessEqualStrategyNumber,
                      dateop::DateLE),
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTEqualStrategyNumber, dateop::DateEQ),
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTGreaterEqualStrategyNumber,
                      dateop::DateGE),
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTGreaterStrategyNumber,
                      dateop::DateGT),

    // oper(int4, int4)
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTLessStrategyNumber, int4op::Int4LT),
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTLessEqualStrategyNumber,
                      int4op::Int4LE),
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTEqualStrategyNumber, int4op::Int4EQ),
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTGreaterEqualStrategyNumber,
                      int4op::Int4GE),
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTGreaterStrategyNumber,
                      int4op::Int4GT),

    // oper(int4, int8)
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTLessStrategyNumber, int4op::Int48LT),
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTLessEqualStrategyNumber,
                      int4op::Int48LE),
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTEqualStrategyNumber, int4op::Int48EQ),
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTGreaterEqualStrategyNumber,
                      int4op::Int48GE),
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTGreaterStrategyNumber,
                      int4op::Int48GT),

    // oper(int4, int2)
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTLessStrategyNumber, int4op::Int42LT),
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTLessEqualStrategyNumber,
                      int4op::Int42LE),
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTEqualStrategyNumber, int4op::Int42EQ),
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTGreaterEqualStrategyNumber,
                      int4op::Int42GE),
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTGreaterStrategyNumber,
                      int4op::Int42GT),

    // oper(int8, int8)
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTLessStrategyNumber, int8op::Int8LT),
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTLessEqualStrategyNumber,
                      int8op::Int8LE),
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTEqualStrategyNumber, int8op::Int8EQ),
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTGreaterEqualStrategyNumber,
                      int8op::Int8GE),
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTGreaterStrategyNumber,
                      int8op::Int8GT),

    // oper(int8, int4)
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTLessStrategyNumber, int8op::Int84LT),
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTLessEqualStrategyNumber,
                      int8op::Int84LE),
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTEqualStrategyNumber, int8op::Int84EQ),
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTGreaterEqualStrategyNumber,
                      int8op::Int84GE),
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTGreaterStrategyNumber,
                      int8op::Int84GT),

    // oper(int8, int2)
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTLessStrategyNumber, int8op::Int82LT),
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTLessEqualStrategyNumber,
                      int8op::Int82LE),
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTEqualStrategyNumber, int8op::Int82EQ),
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTGreaterEqualStrategyNumber,
                      int8op::Int82GE),
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTGreaterStrategyNumber,
                      int8op::Int82GT),

    // oper(float4, float4)
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTLessStrategyNumber,
                      float4op::Float4LT),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTLessEqualStrategyNumber,
                      float4op::Float4LE),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTEqualStrategyNumber,
                      float4op::Float4EQ),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTGreaterEqualStrategyNumber,
                      float4op::Float4GE),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTGreaterStrategyNumber,
                      float4op::Float4GT),

    // oper(float4, float8)
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTLessStrategyNumber,
                      float4op::Float48LT),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTLessEqualStrategyNumber,
                      float4op::Float48LE),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTEqualStrategyNumber,
                      float4op::Float48EQ),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTGreaterEqualStrategyNumber,
                      float4op::Float48GE),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTGreaterStrategyNumber,
                      float4op::Float48GT),

    // oper(float8, float8)
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTLessStrategyNumber,
                      float8op::Float8LT),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTLessEqualStrategyNumber,
                      float8op::Float8LE),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTEqualStrategyNumber,
                      float8op::Float8EQ),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTGreaterEqualStrategyNumber,
                      float8op::Float8GE),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTGreaterStrategyNumber,
                      float8op::Float8GT),

    // oper(float8, float4)
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTLessStrategyNumber,
                      float8op::Float84LT),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTLessEqualStrategyNumber,
                      float8op::Float84LE),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTEqualStrategyNumber,
                      float8op::Float84EQ),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTGreaterEqualStrategyNumber,
                      float8op::Float84GE),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTGreaterStrategyNumber,
                      float8op::Float84GT),

    // oper(text, text)
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTLessStrategyNumber, textop::TextLT),
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTLessEqualStrategyNumber,
                      textop::TextLE),
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTEqualStrategyNumber, textop::TextEQ),
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTGreaterEqualStrategyNumber,
                      textop::TextGE),
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTGreaterStrategyNumber,
                      textop::TextGT),

    // oper(numeric, numeric)
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTLessStrategyNumber,
                      numericop::NumericLT),
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTLessEqualStrategyNumber,
                      numericop::NumericLE),
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTEqualStrategyNumber,
                      numericop::NumericEQ),
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTGreaterEqualStrategyNumber,
                      numericop::NumericGE),
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTGreaterStrategyNumber,
                      numericop::NumericGT),

    // FIXME(jiaqizho): support below oper in the feature
    // BPCHAROID
    // VARCHAROID
    // TIMESTAMPTZOID
    // TIMEOID
    // TIMESTAMPOID
    // DATEOID
    // NAMEOID
    // INT8ARRAYOID,
    // FLOAT8ARRAYOID
    // BYTEAOID
    // OIDOID
    // XIDOID
    // CIDOID
    // REGPROCOID
};

}  // namespace pax