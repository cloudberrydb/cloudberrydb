#ifdef ENABLE_PLASMA
#include "plasma/store.h"
#endif

#include <gtest/gtest.h>

#include <gmock/gmock.h>

#include <thread>

// #include "comm/gtest_wrappers.h"
#include "storage/cache/pax_cache.h"
#include "storage/cache/pax_plasma_cache.h"

#ifdef ENABLE_PLASMA

namespace pax::tests {

static void GenFakeBuffer(char *buffer, size_t length) {
  for (size_t i = 0; i < length; i++) {
    buffer[i] = static_cast<char>(i);
  }
}

#define CACHE_DATA_LEN 100
#define CACHE_META_LEN 20

class PaxCacheTest : public ::testing::Test {
  void SetUp() override {
    plasma_server_ = std::thread([this] {
      plasma::StartServer(
          plasma_socket_ /* socket_name */, "" /* plasma_directory */,
          false /* hugepages_enabled */, nullptr /* external_store */,
          10 * 1024 * 1024 /* system_memory */,
          PLASMA_INFO /* plasmaLogSeverity */);
      plasma::ShutdownServer();
    });
    sleep(1);
  }

  void TearDown() override {
    plasma::StopServer();
    plasma_server_.join();
  }

 protected:
  static void PutKey(PaxCache *pax_cache, const std::string &key,
                     const PaxCache::BatchBuffer &input) {
    auto status = pax_cache->Put(key, input);
    ASSERT_TRUE(status.Ok()) << "fail to put key: " << key << status.Error();
  }

  static void Exist(PaxCache *pax_cache, const std::string &key, bool exist) {
    bool exist_rc = false;
    auto status = pax_cache->Exists(key, &exist_rc);
    ASSERT_TRUE(status.Ok()) << status.Error();
    ASSERT_TRUE(exist ? exist_rc : !exist_rc) << "key: " << key << " exist";
  };

 protected:
  const int64_t client_memory_quota_ = 5 * 1024 * 1024;
  char plasma_socket_[1024] = "/tmp/plasma";
  std::thread plasma_server_;
};

TEST_F(PaxCacheTest, TestCacheInterface) {
  PaxCache *pax_cache;
  PaxPlasmaCache::CacheOptions cache_options;
  PaxCache::Status status;
  PaxCache::BatchBuffer batch_buffer{0};

  cache_options.domain_socket = std::string(plasma_socket_);
  cache_options.client_name = "CLI1";
  cache_options.memory_quota = client_memory_quota_;
  cache_options.waitting_ms = 0;

  pax_cache = new PaxPlasmaCache(cache_options);
  status = pax_cache->Initialize();
  ASSERT_TRUE(status.Ok()) << status.Error();

  // create 3 key
  char data[CACHE_DATA_LEN];
  char meta[CACHE_META_LEN];
  GenFakeBuffer(data, CACHE_DATA_LEN);
  GenFakeBuffer(meta, CACHE_META_LEN);

  batch_buffer.buffer = data;
  batch_buffer.buffer_len = CACHE_DATA_LEN;
  batch_buffer.meta = nullptr;
  batch_buffer.meta = 0;

  PutKey(pax_cache, "key1", batch_buffer);
  batch_buffer.meta = meta;
  batch_buffer.meta_len = CACHE_META_LEN;

  PutKey(pax_cache, "key2", batch_buffer);
  PutKey(pax_cache, "key3", batch_buffer);

  Exist(pax_cache, "key1", true);
  Exist(pax_cache, "key2", true);
  Exist(pax_cache, "key3", true);

  batch_buffer.buffer = nullptr;
  batch_buffer.buffer_len = 0;
  batch_buffer.meta = nullptr;
  batch_buffer.meta_len = 0;

  // get + release
  status = pax_cache->Get("key1", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();
  EXPECT_EQ(CACHE_DATA_LEN, batch_buffer.buffer_len);
  EXPECT_EQ(0, batch_buffer.meta_len);
  EXPECT_EQ(0, std::memcmp(batch_buffer.buffer, data, CACHE_DATA_LEN));
  // still will alloc a meta address with size 0
  EXPECT_NE(nullptr, batch_buffer.meta);

  status = pax_cache->Get("key2", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();
  EXPECT_EQ(CACHE_DATA_LEN, batch_buffer.buffer_len);
  EXPECT_EQ(CACHE_META_LEN, batch_buffer.meta_len);
  EXPECT_EQ(0, std::memcmp(batch_buffer.buffer, data, CACHE_DATA_LEN));
  EXPECT_EQ(0, std::memcmp(batch_buffer.meta, meta, CACHE_META_LEN));

  status = pax_cache->Get("key3", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Release("key1");
  ASSERT_TRUE(status.Ok()) << status.Error();

  std::vector<std::string> release_list = {"key2", "key3"};
  status = pax_cache->Release(release_list);
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Delete("key1");
  ASSERT_TRUE(status.Ok()) << status.Error();

  std::vector<std::string> delete_list = {"key2", "key3"};
  status = pax_cache->Delete(delete_list);
  ASSERT_TRUE(status.Ok()) << status.Error();

  Exist(pax_cache, "key1", false);
  Exist(pax_cache, "key2", false);
  Exist(pax_cache, "key3", false);

  status = pax_cache->Destroy();
  ASSERT_TRUE(status.Ok()) << status.Error();

  delete pax_cache;
}

TEST_F(PaxCacheTest, TestLRUReplace) {
  PaxCache *pax_cache;
  PaxPlasmaCache::CacheOptions cache_options;
  PaxCache::Status status;
  PaxCache::BatchBuffer batch_buffer{0};

  cache_options.domain_socket = std::string(plasma_socket_);
  cache_options.client_name = "CLI1";
  cache_options.memory_quota = CACHE_DATA_LEN * 3;
  cache_options.waitting_ms = 0;

  pax_cache = new PaxPlasmaCache(cache_options);
  status = pax_cache->Initialize();
  ASSERT_TRUE(status.Ok()) << status.Error();

  char data[CACHE_DATA_LEN];
  GenFakeBuffer(data, CACHE_DATA_LEN);

  batch_buffer.buffer = data;
  batch_buffer.buffer_len = CACHE_DATA_LEN;
  batch_buffer.meta = nullptr;
  batch_buffer.meta_len = 0;

  PutKey(pax_cache, "key1", batch_buffer);
  PutKey(pax_cache, "key2", batch_buffer);
  PutKey(pax_cache, "key3", batch_buffer);
  PutKey(pax_cache, "key4", batch_buffer);
  PutKey(pax_cache, "key5", batch_buffer);

  status = pax_cache->Get("key1", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();
  ASSERT_TRUE(batch_buffer.not_exist);

  status = pax_cache->Get("key2", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();
  ASSERT_TRUE(batch_buffer.not_exist);

  status = pax_cache->Get("key3", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();
  ASSERT_FALSE(batch_buffer.not_exist);

  status = pax_cache->Release("key3");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Delete("key3");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Get("key4", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();
  ASSERT_FALSE(batch_buffer.not_exist);

  status = pax_cache->Release("key4");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Delete("key4");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Get("key5", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();
  ASSERT_FALSE(batch_buffer.not_exist);

  status = pax_cache->Release("key5");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Delete("key5");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Destroy();
  ASSERT_TRUE(status.Ok()) << status.Error();

  delete pax_cache;
}

TEST_F(PaxCacheTest, TestGetNoExist) {
  PaxCache *pax_cache;
  PaxPlasmaCache::CacheOptions cache_options;
  PaxCache::Status status;
  PaxCache::BatchBuffer batch_buffer{0};

  cache_options.domain_socket = std::string(plasma_socket_);
  cache_options.client_name = "CLI1";
  cache_options.memory_quota = client_memory_quota_;
  cache_options.waitting_ms = 0;

  pax_cache = new PaxPlasmaCache(cache_options);
  status = pax_cache->Initialize();
  ASSERT_TRUE(status.Ok()) << status.Error();

  char data[CACHE_DATA_LEN];
  char meta[CACHE_META_LEN];
  GenFakeBuffer(data, CACHE_DATA_LEN);
  GenFakeBuffer(meta, CACHE_META_LEN);

  batch_buffer.buffer = data;
  batch_buffer.buffer_len = CACHE_DATA_LEN;
  batch_buffer.meta = meta;
  batch_buffer.meta_len = CACHE_META_LEN;

  PutKey(pax_cache, "key1", batch_buffer);
  Exist(pax_cache, "key1", true);

  status = pax_cache->Get("key1", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Release("key1");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Get("abc", batch_buffer);
  ASSERT_TRUE(status.Ok());
  ASSERT_TRUE(batch_buffer.not_exist);

  std::vector<PaxCache::BatchBuffer> batch_buffers;
  status = pax_cache->Get({"key1", "abc"}, batch_buffers);
  ASSERT_TRUE(status.Ok()) << status.Error();
  ASSERT_FALSE(batch_buffers[0].not_exist);
  ASSERT_TRUE(batch_buffers[1].not_exist);

  status = pax_cache->Release("key1");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Delete("key1");
  ASSERT_TRUE(status.Ok()) << status.Error();

  status = pax_cache->Destroy();
  ASSERT_TRUE(status.Ok()) << status.Error();

  delete pax_cache;
}

TEST_F(PaxCacheTest, TestDifferentClientDelete) {
  PaxCache *pax_cache;
  PaxPlasmaCache::CacheOptions cache_options;
  PaxCache::Status status;
  PaxCache::BatchBuffer batch_buffer{0};

  cache_options.domain_socket = std::string(plasma_socket_);
  cache_options.client_name = "CLI1";
  cache_options.memory_quota = client_memory_quota_;
  cache_options.waitting_ms = 0;

  pax_cache = new PaxPlasmaCache(cache_options);
  status = pax_cache->Initialize();
  ASSERT_TRUE(status.Ok()) << status.Error();

  char data[CACHE_DATA_LEN];
  char meta[CACHE_META_LEN];
  GenFakeBuffer(data, CACHE_DATA_LEN);
  GenFakeBuffer(meta, CACHE_META_LEN);

  batch_buffer.buffer = data;
  batch_buffer.buffer_len = CACHE_DATA_LEN;
  batch_buffer.meta = meta;
  batch_buffer.meta_len = CACHE_META_LEN;

  PutKey(pax_cache, "key1", batch_buffer);
  Exist(pax_cache, "key1", true);

  // CLI1 destroy
  status = pax_cache->Destroy();
  ASSERT_TRUE(status.Ok()) << status.Error();
  delete pax_cache;

  // create CLI2
  cache_options.client_name = "CLI2";
  pax_cache = new PaxPlasmaCache(cache_options);

  status = pax_cache->Initialize();
  ASSERT_TRUE(status.Ok()) << status.Error();

  // check exist
  Exist(pax_cache, "key1", true);

  // get key1
  batch_buffer.buffer = nullptr;
  batch_buffer.buffer_len = 0;
  batch_buffer.meta = nullptr;
  batch_buffer.meta_len = 0;

  status = pax_cache->Get("key1", batch_buffer);
  ASSERT_TRUE(status.Ok()) << status.Error();
  EXPECT_EQ(CACHE_DATA_LEN, batch_buffer.buffer_len);
  EXPECT_EQ(CACHE_META_LEN, batch_buffer.meta_len);
  EXPECT_EQ(0, std::memcmp(batch_buffer.buffer, data, CACHE_DATA_LEN));
  EXPECT_EQ(0, std::memcmp(batch_buffer.meta, meta, CACHE_META_LEN));

  status = pax_cache->Release("key1");
  ASSERT_TRUE(status.Ok()) << status.Error();

  // delete key1
  status = pax_cache->Delete("key1");
  ASSERT_TRUE(status.Ok()) << status.Error();

  // should delete success
  Exist(pax_cache, "key1", false);

  status = pax_cache->Destroy();
  ASSERT_TRUE(status.Ok()) << status.Error();
  delete pax_cache;
}

}  // namespace pax::tests

#endif  //  ENABLE_PLASMA
