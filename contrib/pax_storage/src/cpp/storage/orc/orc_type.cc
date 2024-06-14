#include "storage/orc/orc_type.h"

#include "comm/cbdb_api.h"

#include "exceptions/CException.h"

namespace pax {
pax::porc::proto::Type_Kind ConvertPgTypeToPorcType(FormData_pg_attribute *attr,
                                                    bool is_vec) {
  pax::porc::proto::Type_Kind type;
  if (attr->attbyval) {
    switch (attr->attlen) {
      case 1:
        if (attr->atttypid == BOOLOID) {
          type = pax::porc::proto::Type_Kind::Type_Kind_BOOLEAN;
        } else {
          type = pax::porc::proto::Type_Kind::Type_Kind_BYTE;
        }
        break;
      case 2:
        type = pax::porc::proto::Type_Kind::Type_Kind_SHORT;
        break;
      case 4:
        type = pax::porc::proto::Type_Kind::Type_Kind_INT;
        break;
      case 8:
        type = pax::porc::proto::Type_Kind::Type_Kind_LONG;
        break;
      default:
        CBDB_RAISE(cbdb::CException::kExTypeInvalid);
    }
  } else {
    Assert(attr->attlen > 0 || attr->attlen == -1);
    if (attr->atttypid == NUMERICOID) {
      type = is_vec ? pax::porc::proto::Type_Kind::Type_Kind_VECDECIMAL
                    : pax::porc::proto::Type_Kind::Type_Kind_DECIMAL;
    } else if (attr->atttypid == BPCHAROID) {
      type = is_vec ? pax::porc::proto::Type_Kind::Type_Kind_VECBPCHAR
                    : pax::porc::proto::Type_Kind::Type_Kind_BPCHAR;

    } else {
      type = pax::porc::proto::Type_Kind::Type_Kind_STRING;
    }
  }
  return type;
}
}  // namespace pax
