#include "storage/remote_file_system.h"

namespace pax {
RemoteFile::RemoteFile(pax_fd_handle_t *ht, Oid tbl_space_id,
                       const std::string &file_path)
    : handle_t_(ht),
      ufile_(ht->file),
      tbl_space_id_(tbl_space_id),
      file_path_(file_path) {}

ssize_t RemoteFile::Read(void *ptr, size_t n) const {
  ssize_t ret;
  ret = cbdb::UFileRead(ufile_, (char *)ptr, n);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             UFileGetLastError(ufile_));
  return ret;
}

ssize_t RemoteFile::PRead(void *ptr, size_t n, off_t offset) const {
  ssize_t ret;
  ret = cbdb::UFilePRead(ufile_, (char *)ptr, n, offset);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             UFileGetLastError(ufile_));
  return ret;
}

ssize_t RemoteFile::Write(const void *ptr, size_t n) {
  ssize_t ret;
  ret = cbdb::UFileWrite(ufile_, (char *)ptr, n);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             UFileGetLastError(ufile_));
  return ret;
}
ssize_t RemoteFile::PWrite(const void *ptr, size_t n, off_t offset) {
  ssize_t ret;
  ret = cbdb::UFileWrite(ufile_, (char *)ptr, n);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             UFileGetLastError(ufile_));
  return ret;
}

size_t RemoteFile::FileLength() const {
  int64_t ret;
  ret = cbdb::UFileSize(ufile_);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             UFileGetLastError(ufile_));
  return ret;
}

void RemoteFile::Flush() {
  // httpfs (over s3/oss) does not support flush operations.
  // There are bugs in the implementation of UFileSync in the remote file
  // scenario. For the flush semantics of filesystem, it can be called multiple
  // times. However, in the previous implementation of UFileSync, the file fd
  // was closed after the call, and multiple calls would cause exceptions. So
  // for remote files, flush we do nothing.
}

void RemoteFile::Delete() {
  int ret;
  ret = cbdb::UFileUnlink(tbl_space_id_, file_path_.c_str());
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             UFileGetLastError(ufile_));
  ufile_ = nullptr;
}

void RemoteFile::Close() {
  int ret;
  ret = cbdb::UFileClose(ufile_);

  ForgetFdHandle(handle_t_);
  CBDB_CHECK(ret >= 0, cbdb::CException::kExTypeIOError,
             UFileGetLastError(ufile_));
  ufile_ = nullptr;
}

std::string RemoteFile::GetPath() const { return file_path_; }

// RemoteFileSystem

RemoteFileSystemOptions *RemoteFileSystem::CastOptions(
    FileSystemOptions *options) const {
  CBDB_CHECK(options, cbdb::CException::kExTypeLogicError,
             "open remote file with no options");
  RemoteFileSystemOptions *remote_options =
      dynamic_cast<RemoteFileSystemOptions *>(options);
  CBDB_CHECK(remote_options, cbdb::CException::kExTypeLogicError,
             "open remote file with invalid options");

  return remote_options;
}

File *RemoteFileSystem::Open(const std::string &file_path, int flags,
                             FileSystemOptions *options) {
  auto remote_options = CastOptions(options);

  char errorMessage[UFILE_ERROR_SIZE] = {0};
  UFile *file;
  CBDB_CHECK(!(flags & O_RDWR), cbdb::CException::kExTypeIOError,
             "remote file not support O_RDWR flag");
  // remove O_EXCL flag, because it is not supported by remote file system.
  flags &= ~O_EXCL;
  file = cbdb::UFileOpen(remote_options->tablespace_id_, file_path.c_str(),
                         flags, errorMessage, sizeof(errorMessage));
  CBDB_CHECK(file, cbdb::CException::ExType::kExTypeIOError, errorMessage);
  auto ht = RememberRemoteFileHandle(file);
  CBDB_CHECK(ht, cbdb::CException::ExType::kExTypeIOError);
  return PAX_NEW<RemoteFile>(ht, remote_options->tablespace_id_, file_path);
}
std::string RemoteFileSystem::BuildPath(const File *file) const {
  return file->GetPath();
}
void RemoteFileSystem::Delete(const std::string &file_path,
                              FileSystemOptions *options) const {
  auto remote_options = CastOptions(options);
  cbdb::UFileUnlink(remote_options->tablespace_id_, file_path.c_str());
}

// TODO:(gongxun): use a copy method that is more suitable for object storage
int RemoteFileSystem::CopyFile(const File *src_file, File *dst_file) {
  const size_t kBufSize = 32 * 1024;
  char buf[kBufSize];
  ssize_t num_write = 0;
  ssize_t num_read = 0;

  while ((num_read = src_file->Read(buf, kBufSize)) > 0) {
    num_write = dst_file->Write(buf, num_read);
    CBDB_CHECK(num_write == num_read, cbdb::CException::kExTypeIOError,
               "write error");
  }

  CBDB_CHECK(num_read >= 0, cbdb::CException::kExTypeIOError, "read error");
  return 0;
}

std::vector<std::string> RemoteFileSystem::ListDirectory(
    const std::string &path, FileSystemOptions *options) const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
}

int RemoteFileSystem::CreateDirectory(const std::string &path,
                                      FileSystemOptions *options) const {
  CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
}
void RemoteFileSystem::DeleteDirectory(const std::string &path,
                                       bool delete_topleveldir,
                                       FileSystemOptions *options) const {
  auto remote_options = CastOptions(options);
  cbdb::UFileRmdir(remote_options->tablespace_id_, path.c_str());
}

bool RemoteFileSystem::Exist(const std::string &file_path,
                             FileSystemOptions *options) {
  auto remote_options = CastOptions(options);
  return cbdb::UFileExists(remote_options->tablespace_id_, file_path.c_str());
}

}  // namespace pax
