#pragma once

#include "comm/cbdb_api.h"

#include <map>

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/file_system.h"

namespace pax {
struct pax_fd_handle_t;
class RemoteFileSystem;

class RemoteFile final : public File {
 public:
  RemoteFile(pax_fd_handle_t *ht, Oid tbl_space_id,
             const std::string &file_path);
  ssize_t Read(void *ptr, size_t n) const override;
  ssize_t Write(const void *ptr, size_t n) override;
  ssize_t PWrite(const void *ptr, size_t n, off_t offset) override;
  ssize_t PRead(void *ptr, size_t n, off_t offset) const override;
  size_t FileLength() const override;
  void Flush() override;
  void Delete() override;
  void Close() override;
  std::string GetPath() const override;
  std::string DebugString() const override;

 private:
  pax_fd_handle_t *handle_t_;
  UFile *ufile_;
  Oid tbl_space_id_;
  std::string file_path_;
};

struct RemoteFileSystemOptions final : public FileSystemOptions {
 public:
  RemoteFileSystemOptions() = default;
  RemoteFileSystemOptions(Oid tablespace_id) : tablespace_id_(tablespace_id) {}

  virtual ~RemoteFileSystemOptions() {}
  Oid tablespace_id_;
};

class RemoteFileSystem final : public FileSystem {
  friend class Singleton<RemoteFileSystem>;
  friend class ClassCreator;

 public:
  File *Open(const std::string &file_path, int flags,
             FileSystemOptions *options) override;
  void Delete(const std::string &file_path,
              FileSystemOptions *options) const override;
  std::vector<std::string> ListDirectory(
      const std::string &path, FileSystemOptions *options) const override;
  int CreateDirectory(const std::string &path,
                      FileSystemOptions *options) const override;
  void DeleteDirectory(const std::string &path, bool delete_topleveldir,
                       FileSystemOptions *options) const override;
  bool Exist(const std::string &file_path, FileSystemOptions *options) override;

  // operate with file
  std::string BuildPath(const File *file) const override;
  int CopyFile(const File *src_file, File *dst_file) override;

 private:
  RemoteFileSystem() = default;

  RemoteFileSystemOptions *CastOptions(FileSystemOptions *options) const;
};
}  // namespace pax
