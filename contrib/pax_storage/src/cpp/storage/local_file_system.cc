#include "storage/local_file_system.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "exceptions/CException.h"

namespace paxc {
struct paxc_fd_handle_t {
  int fd;

  // owner of this handle
  ResourceOwner owner;
  struct paxc_fd_handle_t *prev;
  struct paxc_fd_handle_t *next;
};

static struct paxc_fd_handle_t *open_local_fd_handle = NULL;

static struct paxc_fd_handle_t *CreateFdHandle() {
  struct paxc_fd_handle_t *h;

  h = (struct paxc_fd_handle_t *)MemoryContextAlloc(
      TopMemoryContext, sizeof(struct paxc_fd_handle_t));
  h->fd = -1;

  h->owner = CurrentResourceOwner;
  h->next = open_local_fd_handle;
  h->prev = NULL;
  if (open_local_fd_handle) open_local_fd_handle->prev = h;
  open_local_fd_handle = h;

  return h;
}

static void DestroyFdHandle(struct paxc_fd_handle_t *h) {
  int fd = h->fd;

  // unlink from linked list first
  if (h->prev)
    h->prev->next = h->next;
  else
    open_local_fd_handle = h->next;
  if (h->next) h->next->prev = h->prev;

  if (fd != -1) close(fd);

  pfree(h);
}

void FdHandleAbortCallback(ResourceReleasePhase phase, bool is_commit,
                           bool /*isTopLevel*/, void * /*arg*/) {
  struct paxc_fd_handle_t *curr;
  struct paxc_fd_handle_t *next;

  if (phase != RESOURCE_RELEASE_AFTER_LOCKS) return;

  next = open_local_fd_handle;
  while (next) {
    curr = next;
    next = curr->next;

    if (curr->owner == CurrentResourceOwner) {
      if (is_commit)
        elog(WARNING, "pax local fds reference leak: %p still referenced",
             curr);

      DestroyFdHandle(curr);
    }
  }
}

}  // namespace paxc

namespace pax {

LocalFile::LocalFile(int fd, paxc::paxc_fd_handle_t *ht,
                     const std::string &file_path)
    : File(), fd_(fd), handle_t_(ht), file_path_(file_path) {
  Assert(fd >= 0);
  Assert(handle_t_);
}

ssize_t LocalFile::Read(void *ptr, size_t n) {
  ssize_t num;

  do {
    num = read(fd_, ptr, n);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError);
  return num;
}

ssize_t LocalFile::Write(const void *ptr, size_t n) {
  ssize_t num;

  do {
    num = write(fd_, ptr, n);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError);
  return num;
}

ssize_t LocalFile::PRead(void *ptr, size_t n, off_t offset) {
  ssize_t num;

  do {
    num = pread(fd_, ptr, n, offset);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError);
  return num;
}

ssize_t LocalFile::PWrite(const void *ptr, size_t n, off_t offset) {
  ssize_t num;

  do {
    num = pwrite(fd_, ptr, n, offset);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError);
  return num;
}

size_t LocalFile::FileLength() const {
  struct stat file_stat {};

  CBDB_CHECK(fstat(fd_, &file_stat) == 0,
             cbdb::CException::ExType::kExTypeIOError);
  return static_cast<size_t>(file_stat.st_size);
}

void LocalFile::Flush() {
  CBDB_CHECK(fsync(fd_) == 0, cbdb::CException::ExType::kExTypeIOError);
}

void LocalFile::Delete() {
  int rc;

  rc = remove(file_path_.c_str());
  CBDB_CHECK(rc == 0 || errno == ENOENT,
             cbdb::CException::ExType::kExTypeIOError);
}

void LocalFile::Close() {
  int rc;

  do {
    rc = close(fd_);
  } while (unlikely(rc == -1 && errno == EINTR));
  CBDB_CHECK(rc == 0, cbdb::CException::ExType::kExTypeIOError);

  CBDB_WRAP_START;
  {
    handle_t_->fd = -1;
    paxc::DestroyFdHandle(handle_t_);
  }
  CBDB_WRAP_END;
}

std::string LocalFile::GetPath() const { return file_path_; }

File *LocalFileSystem::Open(const std::string &file_path, int flags) {
  LocalFile *local_file;
  int fd;
  paxc::paxc_fd_handle_t *ht;

  if (flags & O_CREAT) {
    fd = open(file_path.c_str(), flags, fs::kDefaultWritePerm);
  } else {
    fd = open(file_path.c_str(), flags);
  }

  CBDB_CHECK(fd >= 0, cbdb::CException::ExType::kExTypeIOError);

  CBDB_WRAP_START;
  {
    ht = paxc::CreateFdHandle();
    Assert(ht);
    ht->fd = fd;
  }
  CBDB_WRAP_END;

  local_file = new LocalFile(fd, ht, file_path);
  return local_file;
}

void LocalFileSystem::Delete(const std::string &file_path) const {
  int rc;

  rc = remove(file_path.c_str());
  CBDB_CHECK(rc == 0 || errno == ENOENT,
             cbdb::CException::ExType::kExTypeIOError);
}

std::string LocalFileSystem::BuildPath(const File *file) const {
  return file->GetPath();
}

std::vector<std::string> LocalFileSystem::ListDirectory(
    const std::string &path) const {
  DIR *dir;
  std::vector<std::string> filelist;
  const char *filepath = path.c_str();

  Assert(filepath != NULL && filepath[0] != '\0');

  dir = opendir(filepath);
  CBDB_CHECK(dir, cbdb::CException::ExType::kExTypeFileOperationError);

  try {
    struct dirent *direntry;
    while ((direntry = readdir(dir)) != NULL) {
      char *filename = &direntry->d_name[0];
      // skip to add '.' or '..' direntry for file enumerating under folder on
      // linux OS.
      if (*filename == '.' &&
          (!strcmp(filename, ".") || !strcmp(filename, "..")))
        continue;
      filelist.push_back(std::string(filename));
    }
  } catch (std::exception &ex) {
    closedir(dir);
    CBDB_RAISE(cbdb::CException::ExType::kExTypeFileOperationError);
  }

  return filelist;
}

void LocalFileSystem::CopyFile(const std::string &src_file_path,
                               const std::string &dst_file_path) const {
  cbdb::CopyFile(src_file_path.c_str(), dst_file_path.c_str());
}

int LocalFileSystem::CreateDirectory(const std::string &path) const {
  return cbdb::PathNameCreateDir(path.c_str());
}

void LocalFileSystem::DeleteDirectory(const std::string &path,
                                      bool delete_topleveldir) const {
  cbdb::PathNameDeleteDir(path.c_str(), delete_topleveldir);
}

}  // namespace pax
