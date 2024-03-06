#pragma once

#include <fcntl.h>

#include <functional>
#include <string>
#include <vector>

namespace pax {

namespace fs {
const int kWriteMode = O_CREAT | O_WRONLY | O_EXCL;
const int kReadMode = O_RDONLY;
const int kReadWriteMode = O_CREAT | O_RDWR | O_EXCL;
const int kDefaultWritePerm = 0640;
};  // namespace fs

/*
 * The IO functions may have error that have two different ways
 * to handle errors. In C style, the function returns -1 and set
 * the errno. The other style likes Java that any error will throw
 * an exception.
 * The IO functions provided by postgres will raise an ERROR
 * if unexpected behavior happens.
 *
 * The following IO functions use the same behavior like postgres,
 * but we throw an exception in C++ code.
 */
class File {
 public:
  virtual ~File() = default;

  // The following [P]Read/[P]Write may partially read/write
  virtual ssize_t Read(void *ptr, size_t n) = 0;
  virtual ssize_t Write(const void *ptr, size_t n) = 0;
  virtual ssize_t PWrite(const void *buf, size_t count, off_t offset) = 0;
  virtual ssize_t PRead(void *buf, size_t count, off_t offset) = 0;

  // The *N version of Read/Write means that R/W must read/write complete
  // number of bytes, or the function should throw an exception.
  // These 4 methods have default implementation that simply calls  read/write
  // and check the returned number of bytes.
  virtual void ReadN(void *ptr, size_t n);
  virtual void WriteN(const void *ptr, size_t n);
  virtual void PWriteN(const void *buf, size_t count, off_t offset);
  virtual void PReadN(void *buf, size_t count, off_t offset);

  virtual void Flush() = 0;
  virtual void Delete() = 0;
  virtual void Close() = 0;
  virtual size_t FileLength() const = 0;
  virtual std::string GetPath() const = 0;
};

class FileSystem {
 public:
  virtual ~FileSystem() = default;
  virtual File *Open(const std::string &file_path, int flags) = 0;
  virtual std::string BuildPath(const File *file) const = 0;
  virtual void Delete(const std::string &file_path) const = 0;
  virtual std::vector<std::string> ListDirectory(
      const std::string &path) const = 0;
  virtual void CopyFile(const std::string &src_file_path,
                        const std::string &dst_file_path) const = 0;
  virtual int CreateDirectory(const std::string &path) const = 0;
  virtual void DeleteDirectory(const std::string &path,
                               bool delete_topleveldir) const = 0;

  void WriteFile(const std::string &file_path, const void *ptr, size_t length);
  void WriteFile(const std::string &file_path,
                 const std::function<void(File *file)> &callback);
};

}  //  namespace pax
