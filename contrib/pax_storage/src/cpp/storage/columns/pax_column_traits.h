#pragma once
#include "storage/columns/pax_column.h"
#include "storage/columns/pax_encoding_column.h"
#include "storage/columns/pax_encoding_non_fixed_column.h"
#include "storage/columns/pax_vec_column.h"
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax::traits {

namespace Impl {

template <typename T>
using CreateFunc = std::function<T *(uint64)>;

template <typename T>
static T *CreateImpl(uint64 cap) {
  auto t = new T(cap);
  return t;
}

template <typename T>
using CreateEncodingFunc =
    std::function<T *(uint64, const PaxEncoder::EncodingOption &)>;

template <typename T>
using CreateDecodingFunc =
    std::function<T *(uint64, const PaxDecoder::DecodingOption &)>;

template <typename T>
static T *CreateEncodingImpl(uint64 cap,
                             const PaxEncoder::EncodingOption &encoding_opt) {
  auto t = new T(cap, encoding_opt);
  return t;
}

template <typename T>
static T *CreateDecodingImpl(uint64 cap,
                             const PaxDecoder::DecodingOption &decoding_opt) {
  auto t = new T(cap, decoding_opt);
  return t;
}

}  // namespace Impl

template <template <typename> class T, typename N>
struct ColumnCreateTraits {};

template <class T>
struct ColumnCreateTraits2 {};

#define TRAITS_DECL(_class, _type)                 \
  template <>                                      \
  struct ColumnCreateTraits<_class, _type> {       \
    static Impl::CreateFunc<_class<_type>> create; \
  }

#define TRAITS_DECL2(_class)                \
  template <>                               \
  struct ColumnCreateTraits2<_class> {      \
    static Impl::CreateFunc<_class> create; \
  }

TRAITS_DECL(PaxCommColumn, int8);
TRAITS_DECL(PaxCommColumn, int16);
TRAITS_DECL(PaxCommColumn, int32);
TRAITS_DECL(PaxCommColumn, int64);
TRAITS_DECL(PaxCommColumn, float);
TRAITS_DECL(PaxCommColumn, double);

TRAITS_DECL(PaxVecCommColumn, int8);
TRAITS_DECL(PaxVecCommColumn, int16);
TRAITS_DECL(PaxVecCommColumn, int32);
TRAITS_DECL(PaxVecCommColumn, int64);
TRAITS_DECL(PaxVecCommColumn, float);
TRAITS_DECL(PaxVecCommColumn, double);

TRAITS_DECL2(PaxNonFixedColumn);
TRAITS_DECL2(PaxVecNonFixedColumn);


template <template <typename> class T, typename N>
struct ColumnOptCreateTraits {};

template <class T>
struct ColumnOptCreateTraits2 {};

#define TRAITS_OPT_DECL(_class, _type)                              \
  template <>                                                       \
  struct ColumnOptCreateTraits<_class, _type> {                     \
    static Impl::CreateEncodingFunc<_class<_type>> create_encoding; \
    static Impl::CreateDecodingFunc<_class<_type>> create_decoding; \
  }

#define TRAITS_OPT_DECL2(_class)                             \
  template <>                                                \
  struct ColumnOptCreateTraits2<_class> {                    \
    static Impl::CreateEncodingFunc<_class> create_encoding; \
    static Impl::CreateDecodingFunc<_class> create_decoding; \
  }

TRAITS_OPT_DECL(PaxEncodingColumn, int8);
TRAITS_OPT_DECL(PaxEncodingColumn, int16);
TRAITS_OPT_DECL(PaxEncodingColumn, int32);
TRAITS_OPT_DECL(PaxEncodingColumn, int64);

TRAITS_OPT_DECL(PaxVecEncodingColumn, int8);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int16);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int32);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int64);

TRAITS_OPT_DECL2(PaxNonFixedEncodingColumn);
TRAITS_OPT_DECL2(PaxVecNonFixedEncodingColumn);

}  // namespace pax::traits