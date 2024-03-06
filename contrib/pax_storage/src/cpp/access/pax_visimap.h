#pragma once

#include <memory>
#include <string>
#include <vector>

#include "comm/cbdb_api.h"

namespace pax {
extern std::shared_ptr<std::vector<uint8>> LoadVisimap(const std::string &visimap_file_path);
extern bool TestVisimap(Relation rel, const char *visimap_name, int offset);

}  // namespace pax
