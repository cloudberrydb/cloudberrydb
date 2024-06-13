#pragma once

#include "comm/cbdb_api.h"

#include <memory>
#include <string>
#include <vector>

#include "storage/file_system.h"

namespace pax {
extern std::shared_ptr<std::vector<uint8>> LoadVisimap(
    FileSystem *fs, FileSystemOptions *options,
    const std::string &visimap_file_path);
extern bool TestVisimap(Relation rel, const char *visimap_name, int offset);

}  // namespace pax
