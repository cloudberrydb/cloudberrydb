#include "storage/file_system.h"

#include "comm/fmt.h"
#include "exceptions/CException.h"

namespace pax {

void File::ReadN(void *ptr, size_t n) const {
  auto num = Read(ptr, n);
  CBDB_CHECK(static_cast<size_t>(num) == n,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to ReadN [require=%lu, read=%ld, errno=%d], %s", n, num,
                 errno, DebugString().c_str()));
}

void File::WriteN(const void *ptr, size_t n) {
  auto num = Write(ptr, n);
  CBDB_CHECK(static_cast<size_t>(num) == n,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to WriteN [require=%lu, written=%ld, errno=%d], %s", n,
                 num, errno, DebugString().c_str()));
}

void File::PReadN(void *buf, size_t count, off64_t offset) const {
  auto num = PRead(buf, count, offset);
  CBDB_CHECK(
      static_cast<size_t>(num) == count,
      cbdb::CException::ExType::kExTypeIOError,
      fmt("Fail to PReadN [offset=%ld, require=%lu, read=%ld, errno=%d], %s",
          offset, count, num, errno, DebugString().c_str()));
}

void File::PWriteN(const void *buf, size_t count, off64_t offset) {
  auto num = PWrite(buf, count, offset);
  CBDB_CHECK(static_cast<size_t>(num) == count,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to PWriteN [offset=%ld, require=%lu, written=%ld, "
                 "errno=%d], %s",
                 offset, count, num, errno, DebugString().c_str()));
}
}  // namespace pax
