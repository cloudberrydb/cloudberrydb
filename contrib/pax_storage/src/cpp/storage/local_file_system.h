#pragma once

#include <sys/stat.h>

#include <string>
#include <utility>
#include <vector>

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"
#include "storage/file_system.h"

namespace pax {

struct pax_fd_handle_t;

class LocalFile final : public File {
 public:
  LocalFile(pax_fd_handle_t *ht, const std::string &file_path);

  ssize_t Read(void *ptr, size_t n) const override;
  ssize_t Write(const void *ptr, size_t n) override;
  ssize_t PWrite(const void *ptr, size_t n, off_t offset) override;
  ssize_t PRead(void *ptr, size_t n, off_t offset) const override;
  size_t FileLength() const override;
  void Flush() override;
  void Delete() override;
  void Close() override;
  std::string GetPath() const override;

 private:
  int fd_;
  pax_fd_handle_t *handle_t_;
  std::string file_path_;
};

class LocalFileSystem final : public FileSystem {
  friend class Singleton<LocalFileSystem>;
  friend class ClassCreator;

 public:
  LocalFileSystem() = default;
  File *Open(const std::string &file_path, int flags,
             FileSystemOptions *options = nullptr) override;
  void Delete(const std::string &file_path,
              FileSystemOptions *options = nullptr) const override;
  std::vector<std::string> ListDirectory(
      const std::string &path,
      FileSystemOptions *options = nullptr) const override;
  int CreateDirectory(const std::string &path,
                      FileSystemOptions *options = nullptr) const override;
  void DeleteDirectory(const std::string &path, bool delete_topleveldir,
                       FileSystemOptions *options = nullptr) const override;
  bool Exist(const std::string &file_path,
             FileSystemOptions *options = nullptr) override;

  // operate with file
  std::string BuildPath(const File *file) const override;

  int CopyFile(const File *src_file, File *dst_file) override;
};
}  // namespace pax
