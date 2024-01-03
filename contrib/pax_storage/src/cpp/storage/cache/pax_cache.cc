#include "storage/cache/pax_cache.h"

namespace pax {

bool PaxCache::Status::Ok() const { return ok_; }

std::string PaxCache::Status::Error() { return error_msg_; }

void PaxCache::Status::SetError(const std::string &error_msg) {
  ok_ = false;
  error_msg_ = error_msg;
}

};  // namespace pax