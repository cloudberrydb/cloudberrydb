#include "storage/file_system_helper.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <mutex>
#include <utility>
#include <vector>

namespace pax {
static struct pax_fd_handle_t *open_fd_handler = NULL;
static std::mutex fd_resouce_owner_mutex;

struct pax_fd_handle_t *AllocFdHandle(FileHandleType type) {
  auto h = (struct pax_fd_handle_t *)malloc(sizeof(struct pax_fd_handle_t));
  if (!h) {
    return NULL;
  }
  h->prev = NULL;
  h->handle_type = type;
  h->owner = CurrentResourceOwner;

  {
    std::lock_guard<std::mutex> lock(fd_resouce_owner_mutex);
    h->next = open_fd_handler;
    if (open_fd_handler) open_fd_handler->prev = h;
    open_fd_handler = h;
  }

  return h;
}

struct pax_fd_handle_t *RememberLocalFdHandle(int fd) {
  auto h = AllocFdHandle(FileHandleType::LocalHandle);
  if (h == NULL) {
    return h;
  }
  h->fd = fd;
  return h;
}

struct pax_fd_handle_t *RememberRemoteFileHandle(UFile *file) {
  auto h = AllocFdHandle(FileHandleType::RemoteHandle);
  if (h == NULL) {
    return h;
  }
  h->file = file;
  return h;
}

void ForgetFdHandle(struct pax_fd_handle_t *h) {
  {
    std::lock_guard<std::mutex> lock(fd_resouce_owner_mutex);

    // unlink from linked list first
    if (h->prev)
      h->prev->next = h->next;
    else
      open_fd_handler = h->next;
    if (h->next) h->next->prev = h->prev;
  }

  free(h);
}

}  // namespace pax

namespace paxc {

void FdHandleAbortCallback(ResourceReleasePhase phase, bool is_commit,
                           bool /*isTopLevel*/, void * /*arg*/) {
  struct pax::pax_fd_handle_t *curr;
  struct pax::pax_fd_handle_t *temp;

  if (phase != RESOURCE_RELEASE_AFTER_LOCKS) return;

  // make sure all of thread have been finished before call this callback
  curr = pax::open_fd_handler;
  AssertImply(curr, !(curr->prev));
  while (curr) {
    temp = curr;
    curr = curr->next;
    // When executing sql containing spi logic, ReleaseCurrentSubTransaction
    // will be called at the end of spi. At this time, FdHandleAbortCallback
    // will be called, We cannot release the parent's resources at this time.
    // so it need to check the owner of the resource.
    if (temp->owner == CurrentResourceOwner) {
      if (is_commit) {
        elog(WARNING, "pax %s files reference leak",
             temp->handle_type == pax::FileHandleType::LocalHandle ? "local"
                                                                   : "remote");
      }

      if (temp->handle_type == pax::FileHandleType::LocalHandle) {
        Assert(temp->fd >= 0);
        close(temp->fd);
      } else {
        Assert(temp->file != nullptr);
        UFileClose(temp->file);
      }

      free(temp);
    }
  }

  pax::open_fd_handler = NULL;
}

}  // namespace paxc