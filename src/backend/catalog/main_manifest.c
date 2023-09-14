/*-------------------------------------------------------------------------
 *
 * main_manifest.c
 *	  save all storage manifest info.
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 *
 * IDENTIFICATION
 *		src/backend/catalog/main_manifest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/main_manifest.h"
#include "utils/rel.h"

/*
 * RemoveMainManifestByRelid
 *      Remove the main manifest record for the relid.
 */
void
RemoveMainManifestByRelid(Oid relid)
{
    Relation    main_manifest;
    HeapTuple   tuple;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];

    main_manifest = table_open(ManifestRelationId, RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_main_manifest_relid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(relid));

    scanDescriptor = systable_beginscan(main_manifest, InvalidOid,
                                        false, NULL, 1, scanKey);

    while (HeapTupleIsValid(tuple = systable_getnext(scanDescriptor)))
    {
        CatalogTupleDelete(main_manifest, &tuple->t_self);
    }

    systable_endscan(scanDescriptor);
    table_close(main_manifest, RowExclusiveLock);
}
