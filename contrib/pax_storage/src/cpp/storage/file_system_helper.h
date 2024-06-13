#include "comm/cbdb_api.h"

namespace paxc {
extern void FdHandleAbortCallback(ResourceReleasePhase phase, bool is_commit,
                                  bool is_top_level, void *arg);
}

namespace pax {

enum FileHandleType {
  LocalHandle = 0,
  RemoteHandle = 1,
};
struct pax_fd_handle_t {
  FileHandleType handle_type;
  union {
    int fd;
    UFile *file;
  };
  ResourceOwner owner;
  struct pax_fd_handle_t *prev;
  struct pax_fd_handle_t *next;
};

struct pax_fd_handle_t *RememberLocalFdHandle(int fd);
struct pax_fd_handle_t *RememberRemoteFileHandle(UFile *file);
void ForgetFdHandle(struct pax_fd_handle_t *h);

}  // namespace pax
