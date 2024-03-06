#include "access/pax_visimap.h"

#include <list>
#include <unordered_map>

#include "comm/bitmap.h"
#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "comm/singleton.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/pax_itemptr.h"

#include <unordered_map>

namespace pax {

template<typename K, typename V>
class LruCache {
public:
  typedef typename std::pair<K, V> key_value_pair_t;
  typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

  struct MemoryContextGuard {
    MemoryContextGuard(MemoryContext ctx) {
      saved = MemoryContextSwitchTo(ctx);
    }
    ~MemoryContextGuard() {
      MemoryContextSwitchTo(saved);
    }
    MemoryContext saved;
  };

  LruCache(size_t max_size) :
    max_size_(max_size) {
  }

  void Put(const K &key, const V &value) {
    auto it = cache_items_map_.find(key);
    cache_items_list_.push_front(key_value_pair_t(key, value));
    if (it != cache_items_map_.end()) {
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
    }
    cache_items_map_[key] = cache_items_list_.begin();

    if (cache_items_map_.size() > max_size_) {
      auto last = cache_items_list_.end();
      last--;
      cache_items_map_.erase(last->first);
      cache_items_list_.pop_back();
    }
  }

  const V Get(const K &key, const std::function<V(const K &)> &load_func) {
    auto it = cache_items_map_.find(key);

// FIXME: The memory of c++ objects is allocated from memory context
// We'll remove the memory context later
    MemoryContextGuard guard(TopMemoryContext);
    if (it == cache_items_map_.end()) {
      CBDB_CHECK(load_func, cbdb::CException::kExTypeLogicError);

      auto value = load_func(key);
      Put(key, value);
      return value;
    } else {
      cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, it->second);
      return it->second->second;
    }
  }

  bool Exists(const K &key) const {
    return cache_items_map_.find(key) != cache_items_map_.end();
  }

  size_t Size() const {
    return cache_items_map_.size();
  }

private:
  std::list<key_value_pair_t> cache_items_list_;
  std::unordered_map<K, list_iterator_t> cache_items_map_;
  size_t max_size_;
};

static LruCache<std::string, std::shared_ptr<std::vector<uint8>>> visimap_cache(16);

std::shared_ptr<std::vector<uint8>> LoadVisimapInternal(const std::string &visimap_file_path) {
  auto fs = pax::Singleton<LocalFileSystem>::GetInstance();
  auto file = fs->Open(visimap_file_path, fs::kReadMode);
  Assert(file);
  size_t file_size = file->FileLength();

  CBDB_CHECK(file_size <= PAX_MAX_NUM_TUPLES_PER_FILE / 8,
             cbdb::CException::kExTypeLogicError);

  std::vector<uint8> buffer(file_size, 0);
  file->ReadN((void *)&buffer[0], file_size);
  file->Close();

  return std::make_shared<std::vector<uint8>>(std::move(buffer));
}

std::shared_ptr<std::vector<uint8>> LoadVisimap(const std::string &visimap_file_path) {
  return visimap_cache.Get(visimap_file_path,
                           [](const std::string &key) {
                              return LoadVisimapInternal(key);
                            });
}

// true: visible
// false: invisible
bool TestVisimap(Relation rel, const char *visimap_name, int offset) {
  auto rel_path = cbdb::BuildPaxDirectoryPath(rel->rd_node, rel->rd_backend);
  auto file_path = cbdb::BuildPaxFilePath(rel_path, visimap_name);
  auto visimap = LoadVisimap(file_path);
  auto bm = Bitmap8(BitmapRaw<uint8>(visimap->data(), visimap->size()), Bitmap8::ReadOnlyOwnBitmap);
  auto is_set = bm.Test(offset);
  return !is_set;
}
}
