#include "storage/toast/pax_toast.h"

#include "comm/pax_memory.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_compress.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

namespace pax {

static std::pair<char *, size_t> pax_pglz_compress_datum_without_hdr(
    const struct varlena *value) {
  int32 valsize, len;
  char *tmp = nullptr;
  PgLZCompressor compressor;

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  if (valsize < PGLZ_strategy_default->min_input_size ||
      valsize > PGLZ_strategy_default->max_input_size)
    return std::make_pair(nullptr, 0);

  len = compressor.GetCompressBound(valsize);
  tmp = PAX_NEW_ARRAY<char>(len);
  len = compressor.Compress(tmp, len, VARDATA_ANY(value), valsize,
                            0 /* level has no effect*/);
  if (compressor.IsError(len)) {
    PAX_DELETE(tmp);
    return std::make_pair(nullptr, 0);
  }
  return std::make_pair(tmp, len);
}

static struct varlena *pax_pglz_compress_datum(const struct varlena *value) {
  int32 valsize, len;
  struct varlena *tmp = nullptr;
  PgLZCompressor compressor;

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  // No point in wasting a alloc cycle if value size is outside the allowed
  // range for compression.
  if (valsize < PGLZ_strategy_default->min_input_size ||
      valsize > PGLZ_strategy_default->max_input_size)
    return nullptr;

  len = compressor.GetCompressBound(valsize);

  // Figure out the maximum possible size of the pglz output, add the bytes
  // that will be needed for varlena overhead, and allocate that amount.
  tmp = (struct varlena *)PAX_NEW_ARRAY<char>(len + VARHDRSZ_COMPRESSED);
  len = compressor.Compress((char *)tmp + VARHDRSZ_COMPRESSED, len,
                            VARDATA_ANY(value), valsize,
                            0 /* level has no effect*/);
  if (compressor.IsError(len)) {
    PAX_DELETE(tmp);
    return nullptr;
  }

  SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

  return tmp;
}

static std::pair<char *, size_t> pax_lz4_compress_datum_without_hdr(
    const struct varlena *value) {
#ifndef USE_LZ4
  CBDB_RAISE(cbdb::CException::kExTypeToastNoLZ4Support);
#else
  int32 valsize;
  int32 len;
  int32 max_size;
  char *tmp = nullptr;
  PaxLZ4Compressor compressor;

  valsize = VARSIZE_ANY_EXHDR(value);
  // Figure out the maximum possible size of the LZ4 output, add the bytes
  // that will be needed for varlena overhead, and allocate that amount.
  max_size = compressor.GetCompressBound(valsize);

  tmp = PAX_NEW_ARRAY<char>(max_size);
  len = compressor.Compress(tmp, max_size, VARDATA_ANY(value), valsize,
                            0 /* level has no effect*/);
  if (compressor.IsError(len) || len > valsize) {
    PAX_DELETE(tmp);
    return std::make_pair(nullptr, 0);
  }

  return std::make_pair(tmp, len);
#endif
}

static struct varlena *pax_lz4_compress_datum(const struct varlena *value) {
#ifndef USE_LZ4
  CBDB_RAISE(cbdb::CException::kExTypeToastNoLZ4Support);
#else
  int32 valsize;
  int32 len;
  int32 max_size;
  struct varlena *tmp = nullptr;
  PaxLZ4Compressor compressor;

  valsize = VARSIZE_ANY_EXHDR(value);
  // Figure out the maximum possible size of the LZ4 output, add the bytes
  // that will be needed for varlena overhead, and allocate that amount.
  max_size = compressor.GetCompressBound(valsize);

  tmp = (struct varlena *)PAX_NEW_ARRAY<char>(max_size + VARHDRSZ_COMPRESSED);

  len = compressor.Compress((char *)tmp + VARHDRSZ_COMPRESSED, max_size,
                            VARDATA_ANY(value), valsize,
                            0 /* level has no effect*/);

  // Should allow current datum compress failed
  // data is incompressible so just free the memory and return NULL
  if (compressor.IsError(len) || len > valsize) {
    PAX_DELETE(tmp);
    return nullptr;
  }

  SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

  return tmp;
#endif
}

static Datum pax_make_compressed_toast(
    Datum value, char cmethod = InvalidCompressionMethod) {
  if (!VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(value)) return value;
  struct varlena *tmp = nullptr;
  uint32 valsize;
  ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;

  Assert(!VARATT_IS_EXTERNAL(DatumGetPointer(value)));
  Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  // If the compression method is not valid, use the current default
  if (!CompressionMethodIsValid(cmethod)) {
    cmethod = default_toast_compression;
  }

  // Call appropriate compression routine for the compression method.
  switch (cmethod) {
    case TOAST_PGLZ_COMPRESSION:
      tmp = pax_pglz_compress_datum((const struct varlena *)value);
      cmid = TOAST_PGLZ_COMPRESSION_ID;
      break;
    case TOAST_LZ4_COMPRESSION:
      tmp = pax_lz4_compress_datum((const struct varlena *)value);
      cmid = TOAST_LZ4_COMPRESSION_ID;
      break;
    default:
      CBDB_RAISE(cbdb::CException::kExTypeToastInvalidCompressType);
  }

  if (tmp == nullptr) return PointerGetDatum(nullptr);

  // We recheck the actual size even if compression reports success, because
  // it might be satisfied with having saved as little as one byte in the
  // compressed data --- which could turn into a net loss once you consider
  // header and alignment padding.  Worst case, the compressed format might
  // require three padding bytes (plus header, which is included in
  // VARSIZE(tmp)), whereas the uncompressed format would take only one
  // header byte and no padding if the value is short enough.  So we insist
  // on a savings of more than 2 bytes to ensure we have a gain.
  if (VARSIZE(tmp) < valsize - 2) {
    // successful compression
    Assert(cmid != TOAST_INVALID_COMPRESSION_ID);
    TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(tmp, valsize, cmid);
    return PointerGetDatum(tmp);
  } else {
    // incompressible data
    PAX_DELETE(tmp);
    return PointerGetDatum(nullptr);
  }
}

static std::tuple<char *, size_t, ToastCompressionId> pax_do_compressed_raw(
    Datum value, char cmethod) {
  ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;
  int32 valsize;
  char *tmp = nullptr;
  size_t tmp_len = 0;

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  // If the compression method is not valid, use the current default
  if (!CompressionMethodIsValid(cmethod)) {
    cmethod = default_toast_compression;
  }

  // Call appropriate compression routine for the compression method.
  // should allow no compress here
  switch (cmethod) {
    case TOAST_PGLZ_COMPRESSION:
      std::tie(tmp, tmp_len) =
          pax_pglz_compress_datum_without_hdr((const struct varlena *)value);
      cmid = TOAST_PGLZ_COMPRESSION_ID;
      break;
    case TOAST_LZ4_COMPRESSION:
      std::tie(tmp, tmp_len) =
          pax_lz4_compress_datum_without_hdr((const struct varlena *)value);
      cmid = TOAST_LZ4_COMPRESSION_ID;
      break;
    default:
      break;
  }

  // compress failed
  if (tmp == nullptr) {
    goto no_compress;
  }

  if (tmp_len >= (uint32)(valsize)-2) {
    // incompressible data
    PAX_DELETE(tmp);
    goto no_compress;
  }

  Assert(cmid != TOAST_INVALID_COMPRESSION_ID);
  return std::make_tuple(tmp, tmp_len, cmid);
no_compress:
  return std::make_tuple(nullptr, 0, TOAST_INVALID_COMPRESSION_ID);
}

static Datum pax_make_external_toast(Datum value,
                                     char cmethod = InvalidCompressionMethod,
                                     bool need_compress = true) {
  char *tmp = nullptr;
  size_t tmp_len = 0;
  ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;
  pax_varatt_external_ref *varatt_ref;
  int32 valsize;

  Assert(!VARATT_IS_EXTERNAL(DatumGetPointer(value)));
  Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  if (!VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(value))
    return PointerGetDatum(nullptr);

  if (need_compress) {
    std::tie(tmp, tmp_len, cmid) = pax_do_compressed_raw(value, cmethod);
  }

  AssertImply(tmp, cmid != TOAST_INVALID_COMPRESSION_ID);
  AssertImply(!tmp, cmid == TOAST_INVALID_COMPRESSION_ID);

  varatt_ref = PAX_NEW<pax_varatt_external_ref>();
  PAX_EXTERNAL_TOAST_SET_SIZE_AND_COMPRESS_METHOD(
      varatt_ref, valsize,
      (cmid != TOAST_INVALID_COMPRESSION_ID) ? tmp_len
                                             : VARSIZE_ANY_EXHDR(value),
      cmid);

  if (cmid != TOAST_INVALID_COMPRESSION_ID) {
    varatt_ref->data_ref = tmp;
    varatt_ref->data_size = tmp_len;
  } else {
    varatt_ref->data_ref = VARDATA_ANY(value);
    varatt_ref->data_size = VARSIZE_ANY_EXHDR(value);
  }

  SET_VARTAG_EXTERNAL(varatt_ref, VARTAG_CUSTOM);
  return PointerGetDatum(varatt_ref);
}

static void pax_free_compress_toast(Datum d) {
  Assert(VARATT_IS_COMPRESSED(d));
  auto ref = DatumGetPointer(d);
  PAX_DELETE(ref);
}

static void pax_free_external_toast(Datum d) {
  Assert(VARATT_IS_PAX_EXTERNAL_TOAST(d));
  auto ref = PaxExtRefGetDatum(d);
  // If current external toast not the compress toast
  // then we should never free it
  if (PAX_VARATT_EXTERNAL_CMID(ref) != TOAST_INVALID_COMPRESSION_ID) {
    PAX_DELETE(ref->data_ref);
  }
  PAX_DELETE(ref);
}

void pax_free_toast(Datum d) {
  if (!d) {
    return;
  }

  if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    pax_free_external_toast(d);
  } else if (VARATT_IS_COMPRESSED(d)) {
    pax_free_compress_toast(d);
  }
}

Datum pax_make_toast(Datum d, char storage_type) {
  Datum result = PointerGetDatum(nullptr);
  if (!pax_enable_toast) {
    return false;
  }

  switch (storage_type) {
    case TYPSTORAGE_PLAIN: {
      // do nothing
      break;
    }
    case TYPSTORAGE_EXTENDED: {
      if (VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(d) &&
          !VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST(d)) {
        result = pax_make_compressed_toast(PointerGetDatum(d));
      } else if (VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST(d)) {
        result = pax_make_external_toast(PointerGetDatum(d));
      }
      break;
    }
    case TYPSTORAGE_EXTERNAL: {
      if (VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST(d)) {
        // should not make compress toast here
        result = pax_make_external_toast(PointerGetDatum(d),
                                         InvalidCompressionMethod, false);
      }
      break;
    }
    case TYPSTORAGE_MAIN: {
      if (VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(d)) {
        result = pax_make_compressed_toast(PointerGetDatum(d));
      }

      break;
    }
    default: {
      Assert(false);
    }
  }

  return result;
}

size_t pax_toast_raw_size(Datum d) {
  if (!VARATT_IS_PAX_SUPPORT_TOAST(d)) return 0;

  if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    return (PaxExtGetDatum(d))->va_extogsz;
  } else if (VARATT_IS_COMPRESSED(d)) {
    return VARDATA_COMPRESSED_GET_EXTSIZE(d);
  }

  pg_unreachable();
}

size_t pax_toast_hdr_size(Datum d) {
  if (!VARATT_IS_PAX_SUPPORT_TOAST(d)) return 0;

  if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    return sizeof(pax_varatt_external);
  } else if (VARATT_IS_COMPRESSED(d)) {
    return VARHDRSZ_COMPRESSED;
  }

  pg_unreachable();
}

size_t pax_decompress_buffer(ToastCompressionId cmid, char *dst_buff,
                             size_t dst_cap, char *src_buff,
                             size_t src_buff_size) {
  switch (cmid) {
    case TOAST_PGLZ_COMPRESSION_ID: {
      size_t rawsize;
      PgLZCompressor compressor;
      // decompress the data
      rawsize =
          compressor.Decompress(dst_buff, dst_cap, src_buff, src_buff_size);
      if (compressor.IsError(rawsize)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeToastPGLZError);
      }

      return rawsize;
    }
    case TOAST_LZ4_COMPRESSION_ID: {
#ifndef USE_LZ4
      CBDB_RAISE(cbdb::CException::kExTypeToastNoLZ4Support);
#else
      size_t rawsize;
      PaxLZ4Compressor compressor;

      rawsize =
          compressor.Decompress(dst_buff, dst_cap, src_buff, src_buff_size);
      if (compressor.IsError(rawsize)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeToastLZ4Error);
      }

      return rawsize;
#endif
    }
    case TOAST_INVALID_COMPRESSION_ID: {
      Assert(dst_cap >= src_buff_size);
      memcpy(dst_buff, src_buff, src_buff_size);
      return src_buff_size;
    }
    default:
      Assert(false);
  }
  pg_unreachable();
}

size_t pax_decompress_datum(Datum d, char *dst_buff, size_t dst_cap) {
  ToastCompressionId cmid;

  Assert(VARATT_IS_COMPRESSED(d));
  Assert(dst_buff);
  // Fetch the compression method id stored in the compression header and
  // decompress the data using the appropriate decompression routine.
  cmid = (ToastCompressionId)(TOAST_COMPRESS_METHOD(d));
  return pax_decompress_buffer(cmid, dst_buff, dst_cap,
                               (char *)d + VARHDRSZ_COMPRESSED,
                               VARSIZE(d) - VARHDRSZ_COMPRESSED);
}

size_t pax_detoast_raw(Datum d, char *dst_buff, size_t dst_cap, char *ext_buff,
                       size_t ext_buff_size) {
  size_t decompress_size = 0;
  if (VARATT_IS_COMPRESSED(d)) {
    auto compress_toast_extsize = VARDATA_COMPRESSED_GET_EXTSIZE(d);
    CBDB_CHECK(dst_cap >= compress_toast_extsize,
               cbdb::CException::ExType::kExTypeOutOfRange);
    // only external toast exist invalid compress toast
    Assert((ToastCompressionId)(TOAST_COMPRESS_METHOD(d)) !=
           TOAST_INVALID_COMPRESSION_ID);

    decompress_size = pax_decompress_datum(d, dst_buff, compress_toast_extsize);
    Assert(decompress_size <= dst_cap);
  } else if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    Assert(ext_buff);
    Assert(ext_buff_size > 0);
    auto offset = PAX_VARATT_EXTERNAL_OFFSET(d);
    auto raw_size = PAX_VARATT_EXTERNAL_SIZE(d);
    auto origin_size = PAX_VARATT_EXTERNAL_ORIGIN_SIZE(d);

    CBDB_CHECK(dst_cap >= origin_size && offset + raw_size <= ext_buff_size,
               cbdb::CException::ExType::kExTypeOutOfRange);

    decompress_size =
        pax_decompress_buffer(PAX_VARATT_EXTERNAL_CMID(d), dst_buff,
                              origin_size, ext_buff + offset, raw_size);
  }

  return decompress_size;
}

Datum pax_detoast(Datum d, char *ext_buff, size_t ext_buff_size) {
  if (VARATT_IS_COMPRESSED(d)) {
    char *result;
    size_t raw_size = VARDATA_COMPRESSED_GET_EXTSIZE(d);
    // allocate memory for the uncompressed data
    result = PAX_NEW_ARRAY<char>(raw_size + VARHDRSZ);
    // only external toast exist invalid compress toast
    Assert((ToastCompressionId)(TOAST_COMPRESS_METHOD(d)) !=
           TOAST_INVALID_COMPRESSION_ID);

    [[maybe_unused]] auto decompress_size =
        pax_decompress_datum(d, result + VARHDRSZ, raw_size);
    Assert(decompress_size == raw_size);

    SET_VARSIZE(result, raw_size + VARHDRSZ);
    return PointerGetDatum(result);
  } else if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    char *result;
    Assert(ext_buff);
    Assert(ext_buff_size > 0);
    auto offset = PAX_VARATT_EXTERNAL_OFFSET(d);
    auto raw_size = PAX_VARATT_EXTERNAL_SIZE(d);
    auto origin_size = PAX_VARATT_EXTERNAL_ORIGIN_SIZE(d);

    CBDB_CHECK(offset + raw_size <= ext_buff_size,
               cbdb::CException::ExType::kExTypeOutOfRange);

    // allocate memory for the uncompressed data
    result = PAX_NEW_ARRAY<char>(origin_size + VARHDRSZ);

    [[maybe_unused]] auto decompress_size =
        pax_decompress_buffer(PAX_VARATT_EXTERNAL_CMID(d), result + VARHDRSZ,
                              origin_size, ext_buff + offset, raw_size);

    Assert(decompress_size == origin_size);
    SET_VARSIZE(result, origin_size + VARHDRSZ);
    return PointerGetDatum(result);
  }

  return d;
}

}  // namespace pax
