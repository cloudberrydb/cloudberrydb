#pragma once
#include "comm/cbdb_api.h"
#include <memory>

//#include "memory_allocator.h"

namespace pax {

template <typename T, typename... Args>
static inline T* PAX_NEW(Args&&... args) {
  return new T(std::forward<Args>(args)...);
}

template <typename T>
static inline T* PAX_NEW_ARRAY(size_t N) {
  return new T[N];
}

template <typename T>
static inline void PAX_DELETE(T *&obj) {
  delete obj;
  obj = nullptr;
}

template <typename T>
static inline void PAX_DELETE_ARRAY(T *&obj) {
  delete []obj;
  obj = nullptr;
}

struct PaxMemoryDeleter {
  template <typename T>
  inline void operator()(T* p) const {
    delete p;
  }
};

template <typename T>
using pax_unique_ptr = std::unique_ptr<T>;

template <typename T>
using pax_shared_ptr = std::shared_ptr<T>;

//template <typename T>
//using pax_unique_ptr = std::unique_ptr<T, PaxMemoryDeleter>;

//template <typename T>
//using pax_shared_ptr = std::shared_ptr<T, PaxMemoryDeleter>;

}

// override the default new/delete to use current memory context
extern void *operator new(std::size_t size);
extern void *operator new[](std::size_t size);
extern void operator delete(void *ptr);
extern void operator delete[](void *ptr);

// specify memory context for this allocation without switching memory context
extern void *operator new(std::size_t size, MemoryContext ctx);
extern void *operator new[](std::size_t size, MemoryContext ctx);
