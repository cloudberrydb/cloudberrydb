#include "storage/cache/pax_plasma_cache.h"

#ifdef ENABLE_PLASMA
#include <plasma/client.h>
#include <plasma/plasma.h>
#endif  // ENABLE_PLASMA

#include <iostream>

#include "comm/cbdb_wrappers.h"

#ifdef ENABLE_PLASMA

namespace pax {

static inline plasma::ObjectID KeyToPlasmaId(const std::string &key,
                                             size_t key_size_limit) {
  plasma::ObjectID key_id;

  Assert(key.length() <= key_size_limit);
  memcpy(key_id.mutable_data(), key.c_str(), key.length());
  memset(key_id.mutable_data() + key.length(), 0,
         key_size_limit - key.length());

  return key_id;
}

static inline std::string PlasmaIdToKey(const plasma::ObjectID &key_id) {
  std::string key;
  key = key_id.binary();
  return key;
}

PaxPlasmaCache::PaxPlasmaCache(const CacheOptions &option)
    : PaxCache(),
      options_(option),
      is_initialized_(false),
      plasma_client_(new plasma::PlasmaClient()) {}

PaxPlasmaCache::~PaxPlasmaCache() { delete plasma_client_; };

PaxCache::Status PaxPlasmaCache::Initialize() {
  PaxCache::Status status;
  if (is_initialized_) {
    status.SetError("Don't initialize twice.");
    return status;
  }

  auto plasma_status = plasma_client_->Connect(
      options_.domain_socket /*store_socket_name*/, "" /*manager_socket_name*/,
      0 /*release_delay*/, 3 /*num_retries*/);
  CHECK_PLASMA_STATUS(plasma_status, status);

  if (options_.memory_quota != 0) {
    plasma_status = plasma_client_->SetClientOptions(options_.client_name,
                                                     options_.memory_quota);
    CHECK_PLASMA_STATUS(plasma_status, status);
  }

  is_initialized_ = true;
  return status;
}

PaxCache::Status PaxPlasmaCache::Put(const std::string &key,
                                     const BatchBuffer &batch_buffer) {
  PaxCache::Status status;
  plasma::ObjectID key_id;
  std::shared_ptr<plasma::Buffer> plasma_buffer;

  assert(is_initialized_);
  assert(key.length() <= KeySizeLimit());
  key_id = KeyToPlasmaId(key, KeySizeLimit());

  plasma::Status plasma_status = plasma_client_->Create(
      key_id, batch_buffer.buffer_len, (const uint8_t *)batch_buffer.meta,
      batch_buffer.meta_len, &plasma_buffer);
  CHECK_PLASMA_STATUS(plasma_status, status);

  assert((size_t)plasma_buffer->size() == batch_buffer.buffer_len);

  memcpy(plasma_buffer->mutable_data(), batch_buffer.buffer,
         batch_buffer.buffer_len);

  plasma_status = plasma_client_->Seal(key_id);
  CHECK_PLASMA_STATUS(plasma_status, status);

  plasma_status = plasma_client_->Release(key_id);
  CHECK_PLASMA_STATUS(plasma_status, status);

  return status;
}

PaxCache::Status PaxPlasmaCache::Put(
    const std::string &key,
    const std::vector<std::pair<char *, size_t>> &buffers,
    const std::pair<char *, size_t> &meta) {
  PaxCache::Status status;
  plasma::ObjectID key_id;
  std::shared_ptr<plasma::Buffer> plasma_buffer;
  size_t total_size = 0;
  size_t data_offset = 0;

  assert(is_initialized_);
  assert(key.length() <= KeySizeLimit());
  key_id = KeyToPlasmaId(key, KeySizeLimit());

  for (auto &pair : buffers) {
    total_size += pair.second;
  }

  plasma::Status plasma_status =
      plasma_client_->Create(key_id, total_size, (const uint8_t *)meta.first,
                             meta.second, &plasma_buffer);
  CHECK_PLASMA_STATUS(plasma_status, status);

  assert((size_t)plasma_buffer->size() == total_size);

  for (auto &pair : buffers) {
    memcpy(plasma_buffer->mutable_data() + data_offset, pair.first,
           pair.second);
    data_offset += pair.second;
  }
  Assert(data_offset == total_size);

  plasma_status = plasma_client_->Seal(key_id);
  CHECK_PLASMA_STATUS(plasma_status, status);

  plasma_status = plasma_client_->Release(key_id);
  CHECK_PLASMA_STATUS(plasma_status, status);
  return status;
}

PaxCache::Status PaxPlasmaCache::Exists(const std::string &key, bool *has) {
  PaxCache::Status status;
  plasma::ObjectID key_id;

  assert(is_initialized_);
  assert(key.length() <= KeySizeLimit());
  key_id = KeyToPlasmaId(key, KeySizeLimit());

  plasma::Status plasma_status = plasma_client_->Contains(key_id, has);
  CHECK_PLASMA_STATUS(plasma_status, status);

  return status;
}

PaxCache::Status PaxPlasmaCache::Get(const std::string &key,
                                     BatchBuffer &batch_buffer) {
  PaxCache::Status status;
  plasma::ObjectID key_id;
  plasma::ObjectBuffer obj_buffer;

  assert(is_initialized_);
  assert(key.length() <= KeySizeLimit());
  key_id = KeyToPlasmaId(key, KeySizeLimit());
  auto plasma_status =
      plasma_client_->Get(&key_id, 1, options_.waitting_ms, &obj_buffer);
  CHECK_PLASMA_STATUS(plasma_status, status);

  if (!obj_buffer.data) {
    // not exist in server
    batch_buffer.not_exist = true;
    return status;
  }

  batch_buffer.buffer = (const char *)obj_buffer.data->data();
  batch_buffer.buffer_len = obj_buffer.data->size();
  batch_buffer.meta = (const char *)obj_buffer.metadata->data();
  batch_buffer.meta_len = obj_buffer.metadata->size();
  batch_buffer.not_exist = false;

  return status;
}

PaxCache::Status PaxPlasmaCache::Get(const std::vector<std::string> &keys,
                                     std::vector<BatchBuffer> &batchs) {
  PaxCache::Status status;
  plasma::ObjectID key_ids[keys.size()];
  plasma::ObjectBuffer obj_buffers[keys.size()];

  assert(is_initialized_);

  for (size_t i = 0; i < keys.size(); i++) {
    assert(keys[i].length() <= KeySizeLimit());
    key_ids[i] = KeyToPlasmaId(keys[i], KeySizeLimit());
  }

  auto plasma_status = plasma_client_->Get(key_ids, keys.size(),
                                           options_.waitting_ms, obj_buffers);
  CHECK_PLASMA_STATUS(plasma_status, status);

  for (size_t i = 0; i < keys.size(); i++) {
    BatchBuffer batch_buffer;
    if (!obj_buffers[i].data) {
      batch_buffer.not_exist = true;
    } else {
      batch_buffer.not_exist = false;
      batch_buffer.buffer = (const char *)obj_buffers[i].data->data();
      batch_buffer.buffer_len = obj_buffers[i].data->size();
      batch_buffer.meta = (const char *)obj_buffers[i].metadata->data();
      batch_buffer.meta_len = obj_buffers[i].metadata->size();
    }
    batchs.emplace_back(batch_buffer);
  }

  return status;
}

PaxCache::Status PaxPlasmaCache::Release(const std::string &key) {
  PaxCache::Status status;
  plasma::ObjectID key_id;

  assert(is_initialized_);
  assert(key.length() <= KeySizeLimit());
  key_id = KeyToPlasmaId(key, KeySizeLimit());
  auto plasma_status = plasma_client_->Release(key_id);
  CHECK_PLASMA_STATUS(plasma_status, status);
  return status;
}

PaxCache::Status PaxPlasmaCache::Release(const std::vector<std::string> &keys) {
  PaxCache::Status status;
  plasma::ObjectID key_id;

  assert(is_initialized_);
  for (const auto &key : keys) {
    key_id = KeyToPlasmaId(key, KeySizeLimit());
    auto plasma_status = plasma_client_->Release(key_id);
    CHECK_PLASMA_STATUS(plasma_status, status);
  }
  return status;
}

PaxCache::Status PaxPlasmaCache::Delete(const std::string &key) {
  PaxCache::Status status;
  plasma::ObjectID key_id;

  assert(is_initialized_);
  assert(key.length() <= KeySizeLimit());
  key_id = KeyToPlasmaId(key, KeySizeLimit());
  plasma::Status plasma_status = plasma_client_->Delete(key_id);
  CHECK_PLASMA_STATUS(plasma_status, status);

  return status;
}

PaxCache::Status PaxPlasmaCache::Delete(const std::vector<std::string> &keys) {
  PaxCache::Status status;
  std::vector<plasma::ObjectID> key_ids;
  std::vector<plasma::ObjectBuffer> obj_buffers;

  assert(is_initialized_);
  for (const auto &key : keys) {
    key_ids.emplace_back(KeyToPlasmaId(key, KeySizeLimit()));
  }

  plasma::Status plasma_status = plasma_client_->Delete(key_ids);
  CHECK_PLASMA_STATUS(plasma_status, status);

  return status;
}

PaxCache::Status PaxPlasmaCache::Destroy() {
  PaxCache::Status status;
  plasma::Status plasma_status = plasma_client_->Disconnect();
  assert(is_initialized_);
  is_initialized_ = false;
  CHECK_PLASMA_STATUS(plasma_status, status);
  return status;
}

size_t PaxPlasmaCache::KeySizeLimit() { return plasma::kUniqueIDSize; }

}  // namespace pax

#endif  // ENABLE_PLASMA