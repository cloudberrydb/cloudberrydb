#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/gp_warehouse.h"

#include "utils/builtins.h"
#include "utils/rel.h"

void
GpWarehouseCreate(char *warehouse_name)
{
 	Relation 	gp_warehouse;
    Oid         warehouseid;
	HeapTuple	tup;
	Datum	 	values[Natts_gp_warehouse];
	bool	 	nulls[Natts_gp_warehouse];

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	gp_warehouse = table_open(GpWarehouseRelationId, RowExclusiveLock);

    warehouseid = GetNewOidWithIndex(gp_warehouse, GpWarehouseOidIndexId,
                                     Anum_gp_warehouse_oid);
    values[Anum_gp_warehouse_oid - 1] = ObjectIdGetDatum(warehouseid);
    values[Anum_gp_warehouse_warehouse_name - 1] = CStringGetTextDatum(warehouse_name);

    tup = heap_form_tuple(RelationGetDescr(gp_warehouse), values, nulls);
    CatalogTupleInsert(gp_warehouse, tup);
    heap_freetuple(tup);

    table_close(gp_warehouse, RowExclusiveLock);
}

Oid
GetGpWarehouseOid(char *warehouse_name, bool missing_ok)
{
    Relation    gp_warehouse;
    ScanKeyData key[1];
    SysScanDesc scan;
    HeapTuple   tuple;
    Oid         warehouseid;

    gp_warehouse = table_open(GpWarehouseRelationId, AccessShareLock);

    ScanKeyInit(&key[0],
                Anum_gp_warehouse_warehouse_name,
                BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(warehouse_name));

    scan = systable_beginscan(gp_warehouse, GpWarehouseNameIndexId, true,
                              NULL, 1, key);

    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple))
        warehouseid = ((Form_gp_warehouse) GETSTRUCT(tuple))->oid;
    else
        warehouseid = InvalidOid;

    systable_endscan(scan);
    table_close(gp_warehouse, AccessShareLock);

    if (!OidIsValid(warehouseid) && !missing_ok)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("warehouse \"%s\" does not exist", warehouse_name)));

    return warehouseid;
}
