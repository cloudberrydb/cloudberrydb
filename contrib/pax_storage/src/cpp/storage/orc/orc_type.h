#pragma once

#include "storage/proto/proto_wrappers.h"
class FormData_pg_attribute;
namespace pax {
extern porc::proto::Type_Kind ConvertPgTypeToPorcType(
    FormData_pg_attribute *attr, bool is_vec);

}  // namespace pax
