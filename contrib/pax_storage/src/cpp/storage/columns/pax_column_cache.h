#pragma once
#include <cstddef>
#include <string>
#include <utility>

#include "storage/columns/pax_columns.h"

#ifdef ENABLE_PLASMA
#include "storage/cache/pax_plasma_cache.h"
namespace pax {

class PaxColumnCache final {
 public:
  PaxColumnCache(PaxCache *cache, const std::string &file_name, bool *proj,
                 size_t proj_num);

  ~PaxColumnCache() = default;

  // Read the cache from current group
  // Return the tuple with
  // <cached PaxColumns, release keyids, a new projection mask>
  std::tuple<PaxColumns *, std::vector<std::string>, bool *> ReadCache(
      size_t group_index);

  void ReleaseCache(std::vector<std::string> keys);

  void WriteCache(PaxColumns *columns, size_t group_index);

 private:
  PaxCache *pax_cache_;
  std::string file_name_;
  bool *proj_;
  size_t proj_num_;
};

};  // namespace pax

#endif  // ENABLE_PLASMA
