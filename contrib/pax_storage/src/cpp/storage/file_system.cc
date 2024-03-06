#include "storage/file_system.h"

#include "exceptions/CException.h"
namespace pax {

void File::ReadN(void *ptr, size_t n) {
  auto num = Read(ptr, n);
  CBDB_CHECK(static_cast<size_t>(num) == n,
             cbdb::CException::ExType::kExTypeIOError);
}

void File::WriteN(const void *ptr, size_t n) {
  auto num = Write(ptr, n);
  CBDB_CHECK(static_cast<size_t>(num) == n,
             cbdb::CException::ExType::kExTypeIOError);
}

void File::PReadN(void *buf, size_t count, off_t offset) {
  auto num = PRead(buf, count, offset);
  CBDB_CHECK(static_cast<size_t>(num) == count,
             cbdb::CException::ExType::kExTypeIOError);
}

void File::PWriteN(const void *buf, size_t count, off_t offset) {
  auto num = PWrite(buf, count, offset);
  CBDB_CHECK(static_cast<size_t>(num) == count,
             cbdb::CException::ExType::kExTypeIOError);
}

void FileSystem::WriteFile(const std::string &file_path,
                           const std::function<void(File *file)> &callback) {
  auto file = Open(file_path, pax::fs::kWriteMode);
  callback(file);
  file->Close();
}

void FileSystem::WriteFile(const std::string &file_path, const void *ptr,
                           size_t length) {
  WriteFile(file_path, [=](File *file) { file->WriteN(ptr, length); });
}
}  // namespace pax
