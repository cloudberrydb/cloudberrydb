#ifndef GP_WAREHOUSE_H
#define GP_WAREHOUSE_H

#include "catalog/genbki.h"
#include "catalog/gp_warehouse_d.h"

/* ----------------
 *		gp_warehouse definition.  cpp turns this into
 *		typedef struct FormData_gp_warehouse
 * ----------------
 */
CATALOG(gp_warehouse,8690,GpWarehouseRelationId) BKI_SHARED_RELATION
{
    Oid		oid		BKI_FORCE_NOT_NULL;	/* oid */
    int32	warehouse_size;				/* warehouse size */
    text	warehouse_name	BKI_FORCE_NOT_NULL;	/* warehouse name */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
    text    status          BKI_FORCE_NOT_NULL; /* status */
#endif
} FormData_gp_warehouse;

typedef FormData_gp_warehouse *Form_gp_warehouse;

DECLARE_UNIQUE_INDEX(gp_warehouse_oid_index, 8086, on gp_warehouse using btree(oid oid_ops));
#define GpWarehouseOidIndexId	8086
DECLARE_UNIQUE_INDEX(gp_warehouse_name_index, 8059, on gp_warehouse using btree(warehouse_name text_ops));
#define GpWarehouseNameIndexId	8059

#endif   /* GP_WAREHOUSE_H */
