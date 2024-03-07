#pragma once
#include "comm/cbdb_api.h"

#include <functional>
#include <map>

namespace pax {

using OperMinMaxFunc = std::function<bool(const void *, const void *, Oid)>;
using OperMinMaxKey = std::tuple<Oid, Oid, StrategyNumber>;

namespace boolop {
extern bool BoolLT(const void *l, const void *r, Oid /*collation*/);
extern bool BoolLE(const void *l, const void *r, Oid /*collation*/);
extern bool BoolEQ(const void *l, const void *r, Oid /*collation*/);
extern bool BoolGE(const void *l, const void *r, Oid /*collation*/);
extern bool BoolGT(const void *l, const void *r, Oid /*collation*/);
}  // namespace boolop

namespace charop {
extern bool CharLT(const void *l, const void *r, Oid /*collation*/);
extern bool CharLE(const void *l, const void *r, Oid /*collation*/);
extern bool CharEQ(const void *l, const void *r, Oid /*collation*/);
extern bool CharGE(const void *l, const void *r, Oid /*collation*/);
extern bool CharGT(const void *l, const void *r, Oid /*collation*/);

}  // namespace charop

namespace int2op {
extern bool Int2LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int2LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int2EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int2GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int2GT(const void *l, const void *r, Oid /*collation*/);

extern bool Int24LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int24LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int24EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int24GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int24GT(const void *l, const void *r, Oid /*collation*/);

extern bool Int28LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int28LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int28EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int28GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int28GT(const void *l, const void *r, Oid /*collation*/);
}  // namespace int2op

namespace dateop {
extern bool DateLT(const void *l, const void *r, Oid /*collation*/);
extern bool DateLE(const void *l, const void *r, Oid /*collation*/);
extern bool DateEQ(const void *l, const void *r, Oid /*collation*/);
extern bool DateGE(const void *l, const void *r, Oid /*collation*/);
extern bool DateGT(const void *l, const void *r, Oid /*collation*/);

}  // namespace dateop

namespace int4op {
extern bool Int4LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int4LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int4EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int4GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int4GT(const void *l, const void *r, Oid /*collation*/);

extern bool Int48LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int48LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int48EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int48GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int48GT(const void *l, const void *r, Oid /*collation*/);

extern bool Int42LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int42LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int42EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int42GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int42GT(const void *l, const void *r, Oid /*collation*/);
}  // namespace int4op

namespace int8op {
extern bool Int8LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int8LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int8EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int8GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int8GT(const void *l, const void *r, Oid /*collation*/);

extern bool Int84LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int84LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int84EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int84GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int84GT(const void *l, const void *r, Oid /*collation*/);

extern bool Int82LT(const void *l, const void *r, Oid /*collation*/);
extern bool Int82LE(const void *l, const void *r, Oid /*collation*/);
extern bool Int82EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Int82GE(const void *l, const void *r, Oid /*collation*/);
extern bool Int82GT(const void *l, const void *r, Oid /*collation*/);
}  // namespace int8op

namespace float4op {
extern bool Float4LT(const void *l, const void *r, Oid /*collation*/);
extern bool Float4LE(const void *l, const void *r, Oid /*collation*/);
extern bool Float4EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Float4GE(const void *l, const void *r, Oid /*collation*/);
extern bool Float4GT(const void *l, const void *r, Oid /*collation*/);

extern bool Float48LT(const void *l, const void *r, Oid /*collation*/);
extern bool Float48LE(const void *l, const void *r, Oid /*collation*/);
extern bool Float48EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Float48GE(const void *l, const void *r, Oid /*collation*/);
extern bool Float48GT(const void *l, const void *r, Oid /*collation*/);
}  // namespace float4op

namespace float8op {
extern bool Float8LT(const void *l, const void *r, Oid /*collation*/);
extern bool Float8LE(const void *l, const void *r, Oid /*collation*/);
extern bool Float8EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Float8GE(const void *l, const void *r, Oid /*collation*/);
extern bool Float8GT(const void *l, const void *r, Oid /*collation*/);

extern bool Float84LT(const void *l, const void *r, Oid /*collation*/);
extern bool Float84LE(const void *l, const void *r, Oid /*collation*/);
extern bool Float84EQ(const void *l, const void *r, Oid /*collation*/);
extern bool Float84GE(const void *l, const void *r, Oid /*collation*/);
extern bool Float84GT(const void *l, const void *r, Oid /*collation*/);
}  // namespace float8op

namespace textop {
extern bool TextLT(const void *l, const void *r, Oid collation);
extern bool TextLE(const void *l, const void *r, Oid collation);
extern bool TextEQ(const void *l, const void *r, Oid collation);
extern bool TextGE(const void *l, const void *r, Oid collation);
extern bool TextGT(const void *l, const void *r, Oid collation);

extern bool BpCharLT(const void *l, const void *r, Oid /*collation*/);
extern bool BpCharLE(const void *l, const void *r, Oid /*collation*/);
extern bool BpCharEQ(const void *l, const void *r, Oid /*collation*/);
extern bool BpCharGE(const void *l, const void *r, Oid /*collation*/);
extern bool BpCharGT(const void *l, const void *r, Oid /*collation*/);

}  // namespace textop

namespace numericop {
extern bool NumericLT(const void *l, const void *r, Oid collation);
extern bool NumericLE(const void *l, const void *r, Oid collation);
extern bool NumericEQ(const void *l, const void *r, Oid collation);
extern bool NumericGE(const void *l, const void *r, Oid collation);
extern bool NumericGT(const void *l, const void *r, Oid collation);
}  // namespace numericop

extern std::map<OperMinMaxKey, OperMinMaxFunc> min_max_opers;

}  // namespace pax