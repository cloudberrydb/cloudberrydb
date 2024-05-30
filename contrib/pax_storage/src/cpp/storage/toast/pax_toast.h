#pragma once
#include "comm/cbdb_api.h"

#include "comm/guc.h"

namespace pax {

typedef struct pax_varatt_external {
  int32 va_rawsize;  /* Original data size (doesn't includes header) */
  uint32 va_extinfo; /* External saved size (without header) and
                      * compression method */
  uint64 va_extogsz;
  uint64 va_extoffs;
  uint64 va_extsize;
} pax_varatt_external;

static_assert(sizeof(pax_varatt_external) == 32, "illegal structure");

#define PAX_VARATT_EXTERNAL_CMID(PTR)                       \
  (ToastCompressionId)((PaxExtGetDatum(PTR))->va_extinfo >> \
                       VARLENA_EXTSIZE_BITS)

#define PAX_VARATT_EXTERNAL_ORIGIN_SIZE(PTR) ((PaxExtGetDatum(PTR))->va_extogsz)

#define PAX_VARATT_EXTERNAL_OFFSET(PTR) ((PaxExtGetDatum(PTR))->va_extoffs)

#define PAX_VARATT_EXTERNAL_SIZE(PTR) ((PaxExtGetDatum(PTR))->va_extsize)

typedef struct pax_varatt_external_ref {
  pax_varatt_external ext;

  char *data_ref;
  size_t data_size;
} pax_varatt_external_ref;

#define PaxExtGetDatum(PTR) (pax_varatt_external *)(PTR)
#define PaxExtRefGetDatum(PTR) (pax_varatt_external_ref *)(PTR)

// pax only accepts compressed and external toast
#define VARATT_IS_PAX_EXTERNAL_TOAST(PTR) \
  (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_CUSTOM)

#define VARATT_IS_PAX_SUPPORT_TOAST(PTR) \
  (VARATT_IS_PAX_EXTERNAL_TOAST(PTR) || VARATT_IS_COMPRESSED(PTR))

#define PAX_VARSIZE_ANY(PTR)                                       \
  (VARATT_IS_PAX_EXTERNAL_TOAST(PTR) ? sizeof(pax_varatt_external) \
                                     : VARSIZE_ANY(PTR))

#define PAX_EXTERNAL_TOAST_SET_SIZE_AND_COMPRESS_METHOD(            \
    PTR, origin_len, compress_len, cm_method)                       \
  do {                                                              \
    Assert((origin_len) > 0 && (uint64)(origin_len) <= UINT64_MAX); \
    Assert((cm_method) == TOAST_PGLZ_COMPRESSION_ID ||              \
           (cm_method) == TOAST_LZ4_COMPRESSION_ID ||               \
           (cm_method) == TOAST_INVALID_COMPRESSION_ID);            \
    (PaxExtGetDatum(PTR))->va_extinfo =                             \
        (0) | ((uint32)(cm_method) << VARLENA_EXTSIZE_BITS);        \
    (PaxExtGetDatum(PTR))->va_extogsz = origin_len;                 \
    (PaxExtGetDatum(PTR))->va_extsize = compress_len;               \
  } while (0)

#define PAX_VARATT_EXTERNAL_SET_OFFSET(PTR, offs) \
  do {                                            \
    Assert(VARATT_IS_PAX_EXTERNAL_TOAST(PTR));    \
    (PaxExtGetDatum(PTR))->va_extoffs = offs;     \
  } while (0)

#define VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(PTR) \
  (pax_enable_toast &&                            \
   VARSIZE_ANY_EXHDR(PTR) >= (uint32)pax_min_size_of_compress_toast)

#define VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST_BY_SIZE(SIZE) \
  (pax_enable_toast && SIZE >= (uint32)pax_min_size_of_compress_toast)

#define VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST(PTR) \
  (pax_enable_toast &&                          \
   VARSIZE_ANY_EXHDR(PTR) >= (uint32)pax_min_size_of_external_toast)

#define VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST_BY_SIZE(SIZE) \
  (pax_enable_toast && SIZE >= (uint32)pax_min_size_of_external_toast)

// make pax toast
Datum pax_make_toast(Datum d, char storage_type);

// raw size of pax toast
size_t pax_toast_raw_size(Datum d);
size_t pax_toast_hdr_size(Datum d);

// detoast pax toast
size_t pax_detoast_raw(Datum d, char *dst_buff, size_t dst_size,
                       char *ext_buff = nullptr, size_t ext_buff_size = 0);
Datum pax_detoast(Datum d, char *ext_buff = nullptr, size_t ext_buff_size = 0);

// free pax toast
void pax_free_toast(Datum d);

}  // namespace pax
