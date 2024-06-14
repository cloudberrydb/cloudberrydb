#pragma once

#ifdef VEC_BUILD

#include "comm/bitmap.h"
#include "storage/columns/pax_column.h"
#include "storage/pax_buffer.h"
#include "storage/pax_defined.h"

namespace pax {

void CopyBitmap(const Bitmap8 *bitmap, size_t range_begin, size_t range_lens,
                DataBuffer<char> *null_bits_buffer);

void VarlenaToRawBuffer(char *buffer, size_t buffer_len, char **out_data,
                        size_t *out_len);

void CopyBitmapBuffer(PaxColumn *column,
                      std::shared_ptr<Bitmap8> visibility_map_bitset,
                      size_t bitset_index_begin, size_t range_begin,
                      size_t range_lens, size_t data_range_lens,
                      size_t out_range_lens, DataBuffer<char> *null_bits_buffer,
                      size_t *out_visable_null_counts);

}  // namespace pax
#endif  // VEC_BUILD
