/*-------------------------------------------------------------------------
 *
 * gp_storage_user_mapping.h
 *
 * Portions Copyright (c) 2024, HashData Technology Limited.
 *
 * IDENTIFICATION
 *          src/include/catalog/gp_storage_user_mapping.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_STORAGE_USER_MAPPING_H
#define GP_STORAGE_USER_MAPPING_H

#include "catalog/genbki.h"
#include "catalog/gp_storage_user_mapping_d.h"

/* ----------------
 *		gp_storage_user_mapping definition.  cpp turns this into
 *		typedef struct FormData_gp_storage_user_mapping
 * ----------------
 */
CATALOG(gp_storage_user_mapping,6131,StorageUserMappingRelationId) BKI_SHARED_RELATION
{
	Oid			oid;			/* oid */

	Oid			umuser BKI_LOOKUP_OPT(pg_authid);	/* Id of the user,
													 * InvalidOid if PUBLIC is
													 * wanted */
	Oid			umserver BKI_LOOKUP(gp_storage_server); /* server of this
														 * mapping */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		umoptions[1];	/* user mapping options */
#endif
} FormData_gp_storage_user_mapping;

/* ----------------
 *		Form_gp_storage_user_mapping corresponds to a pointer to a tuple with
 *		the format of gp_storage_user_mapping relation.
 * ----------------
 */
typedef FormData_gp_storage_user_mapping *Form_gp_storage_user_mapping;

DECLARE_TOAST(gp_storage_user_mapping, 6132, 6133);
#define GpStorageUserMappingToastTable	6132
#define GpStorageUserMappingToastIndex	6133

DECLARE_UNIQUE_INDEX_PKEY(gp_storage_user_mapping_oid_index, 6134, on gp_storage_user_mapping using btree(oid oid_ops));
#define StorageUserMappingOidIndexId	6134
DECLARE_UNIQUE_INDEX(gp_storage_user_mapping_server_index, 6135, on gp_storage_user_mapping using btree(umuser oid_ops, umserver oid_ops));
#define StorageUserMappingServerIndexId	6135

#endif //GP_STORAGE_USER_MAPPING_H
