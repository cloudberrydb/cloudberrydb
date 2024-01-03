#pragma once

#ifdef ENABLE_PLASMA

#include <string>
#include <string_view>

#include "storage/cache/pax_cache.h"

namespace plasma {
class PlasmaClient;
}

namespace pax {

#define CHECK_PLASMA_STATUS(plasma_status, status_rc)   \
  do {                                                  \
    if (!(plasma_status).ok()) {                        \
      (status_rc).SetError((plasma_status).ToString()); \
      return (status_rc);                               \
    }                                                   \
  } while (0);

class PaxPlasmaCache : public PaxCache {
 public:
  struct CacheOptions {
    std::string domain_socket;
    // client name + memory quota will limit current client memory used
    // if memory_quota_ is 0 means no limit
    // Notice that: if current plasma server capcity LT memory quota
    // Then it will make Initialize failed
    std::string client_name;
    size_t memory_quota = 0;

    // the waitting time after `Get` call failed
    // during this period, if the same `key` is put,
    // the data will be obtained
    size_t waitting_ms = 0;
  };

  explicit PaxPlasmaCache(const CacheOptions &option);

  ~PaxPlasmaCache() override;

  PaxCache::Status Initialize() override;

  PaxCache::Status Put(const std::string &key,
                       const BatchBuffer &batch_buffer) override;

  PaxCache::Status Put(const std::string &key,
                       const std::vector<std::pair<char *, size_t>> &buffers,
                       const std::pair<char *, size_t> &meta) override;

  PaxCache::Status Exists(const std::string &key, bool *has) override;

  PaxCache::Status Get(const std::string &key,
                       BatchBuffer &batch_buffer) override;

  PaxCache::Status Get(const std::vector<std::string> &keys,
                       std::vector<BatchBuffer> &batchs) override;

  PaxCache::Status Release(const std::string &key) override;

  PaxCache::Status Release(const std::vector<std::string> &keys) override;

  PaxCache::Status Delete(const std::string &key) override;

  PaxCache::Status Delete(const std::vector<std::string> &keys) override;

  PaxCache::Status Destroy() override;

  size_t KeySizeLimit() override;

 private:
  CacheOptions options_;
  bool is_initialized_;
  plasma::PlasmaClient *plasma_client_;
};

}  // namespace pax

#endif  // ENABLE_PLASMA
