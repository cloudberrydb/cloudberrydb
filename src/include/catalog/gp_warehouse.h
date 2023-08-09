#ifndef GP_WAREHOUSE_H
#define GP_WAREHOUSE_H

#include "catalog/genbki.h"
#include "catalog/gp_warehouse_d.h"

/*
 * Defines for gp_version_at_initdb table
 */
CATALOG(gp_warehouse,8690,GpWarehouseRelationId) BKI_SHARED_RELATION
{
    Oid			oid;			/* oid */
    text        warehouse_name; /* warehouse name */
} FormData_gp_warehouse;

typedef FormData_gp_warehouse *Form_gp_warehouse;

DECLARE_UNIQUE_INDEX(gp_warehouse_oid_index, 8086, on gp_warehouse using btree(oid oid_ops));
#define GpWarehouseOidIndexId	8086
DECLARE_UNIQUE_INDEX(gp_warehouse_name_index, 8059, on gp_warehouse using btree(warehouse_name text_ops));
#define GpWarehouseNameIndexId	8059

extern Oid GetGpWarehouseOid(char *warehouse_name, bool missing_ok);

#endif   /* GP_WAREHOUSE_H */
