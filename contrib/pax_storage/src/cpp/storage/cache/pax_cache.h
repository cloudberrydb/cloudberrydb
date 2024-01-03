#pragma once

#include <string>
#include <vector>

namespace pax {

class PaxCache {
 public:
  struct Status {
    friend class PaxCache;

    bool Ok() const;

    std::string Error();

    void SetError(const std::string &error_msg);

   private:
    bool ok_ = true;
    std::string error_msg_;
  };

  struct BatchBuffer {
    const char *buffer = nullptr;
    size_t buffer_len = 0;
    const char *meta = nullptr;
    size_t meta_len = 0;

    bool not_exist = false;
  };

  virtual ~PaxCache() = default;

  virtual Status Initialize() = 0;

  virtual Status Put(const std::string &key,
                     const BatchBuffer &batch_buffer) = 0;

  virtual Status Put(const std::string &key,
                     const std::vector<std::pair<char *, size_t>> &buffers,
                     const std::pair<char *, size_t> &meta) = 0;

  virtual Status Exists(const std::string &key, bool *has) = 0;

  virtual Status Get(const std::string &key, BatchBuffer &batch_buffer) = 0;

  virtual Status Get(const std::vector<std::string> &keys,
                     std::vector<BatchBuffer> &batchs) = 0;

  virtual Status Release(const std::string &key) = 0;

  virtual Status Release(const std::vector<std::string> &keys) = 0;

  virtual Status Delete(const std::string &key) = 0;

  virtual Status Delete(const std::vector<std::string> &key) = 0;

  virtual Status Destroy() = 0;

  virtual size_t KeySizeLimit() = 0;
};

}  // namespace pax
