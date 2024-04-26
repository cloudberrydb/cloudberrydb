#include "comm/pax_memory.h"

#include "comm/cbdb_wrappers.h"

void *operator new(std::size_t size) { return cbdb::Palloc(size); }

void *operator new[](std::size_t size) { return cbdb::Palloc(size); }

void *operator new(std::size_t size, MemoryContext ctx) {
  return cbdb::MemCtxAlloc(ctx, size);
}

void *operator new[](std::size_t size, MemoryContext ctx) {
  return cbdb::MemCtxAlloc(ctx, size);
}

void operator delete(void *ptr) {
  if (ptr) cbdb::Pfree(ptr);
}

void operator delete(void* ptr, size_t size) {
  if (ptr) cbdb::Pfree(ptr);
}

void operator delete[](void *ptr) {
  if (ptr) cbdb::Pfree(ptr);
}

void operator delete[](void *ptr, size_t size) {
  if (ptr) cbdb::Pfree(ptr);
}


