#include "storage/columns/pax_compress.h"

#include <random>

#include "comm/cbdb_wrappers.h"
#include "comm/gtest_wrappers.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_encoding_utils.h"

namespace pax::tests {
class PaxCompressTest : public ::testing::TestWithParam<
                            ::testing::tuple<ColumnEncoding_Kind, uint32>> {
  void SetUp() override {
    MemoryContext pax_compress_memory_context = AllocSetContextCreate(
        (MemoryContext)NULL, "PaxCompressTestMemoryContext", 200 * 1024 * 1024,
        200 * 1024 * 1024, 200 * 1024 * 1024);

    MemoryContextSwitchTo(pax_compress_memory_context);
  }
};

TEST_P(PaxCompressTest, TestCompressAndDecompress) {
  ColumnEncoding_Kind type = ::testing::get<0>(GetParam());
  uint32 data_len = ::testing::get<1>(GetParam());
  size_t dst_len = 0;
  PaxCompressor *compressor;

  char *data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  char *result_data = reinterpret_cast<char *>(cbdb::Palloc(data_len));
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = i;
  }

  compressor = PaxCompressor::CreateBlockCompressor(type);

  size_t bound_size = compressor->GetCompressBound(data_len);  // NOLINT
  ASSERT_GT(bound_size, 0);
  result_data =
      reinterpret_cast<char *>(cbdb::RePalloc(result_data, bound_size));
  dst_len = bound_size;
  dst_len = compressor->Compress(result_data, dst_len, data, data_len, 1);
  ASSERT_FALSE(compressor->IsError(dst_len));
  ASSERT_GT(dst_len, 0);

  // reset data
  for (size_t i = 0; i < data_len; ++i) {
    data[i] = 0;
  }

  size_t decompress_len =
      compressor->Decompress(data, data_len, result_data, dst_len);
  ASSERT_GT(decompress_len, 0);
  ASSERT_EQ(decompress_len, data_len);
  for (size_t i = 0; i < data_len; ++i) {
    ASSERT_EQ(data[i], (char)i);
  }

  delete compressor;
  delete data;
  delete result_data;
}

INSTANTIATE_TEST_CASE_P(
    PaxCompressTestCombined, PaxCompressTest,
    testing::Combine(testing::Values(ColumnEncoding_Kind_COMPRESS_ZSTD,
                                     ColumnEncoding_Kind_COMPRESS_ZLIB),
                     testing::Values(1, 128, 4096, 1024 * 1024,
                                     64 * 1024 * 1024)));

}  // namespace pax::tests
