/*-------------------------------------------------------------------------
 *
 * gp_storage_server.h
 *
 * Portions Copyright (c) 2024, HashData Technology Limited.
 *
 * IDENTIFICATION
 *          src/include/catalog/gp_storage_server.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_STORAGE_SERVER_H
#define GP_STORAGE_SERVER_H

#include "catalog/genbki.h"
#include "catalog/gp_storage_server_d.h"

/* ----------------
 *		gp_storage_server definition.  cpp turns this into
 *		typedef struct FormData_gp_storage_server
 * ----------------
 */
CATALOG(gp_storage_server,6015,StorageServerRelationId) BKI_SHARED_RELATION
{
	Oid			oid;			/* oid */
	NameData	srvname;		/* storage server name */
	Oid			srvowner BKI_LOOKUP(pg_authid); /* server owner */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	aclitem		srvacl[1];		/* access permissions */
	text		srvoptions[1];	/* FDW-specific options */
#endif
} FormData_gp_storage_server;

/* ----------------
 *		Form_gp_storage_server corresponds to a pointer to a tuple with
 *		the format of gp_storage_server relation.
 * ----------------
 */
typedef FormData_gp_storage_server *Form_gp_storage_server;

DECLARE_TOAST(gp_storage_server, 6016, 6017);
#define GpStorageServerToastTable	6016
#define GpStorageServerToastIndex	6017

DECLARE_UNIQUE_INDEX_PKEY(gp_storage_server_oid_index, 6018, on gp_storage_server using btree(oid oid_ops));
#define StorageServerOidIndexId	6018
DECLARE_UNIQUE_INDEX(gp_storage_server_name_index, 6019, on gp_storage_server using btree(srvname name_ops));
#define StorageServerNameIndexId	6019

#endif //GP_STORAGE_SERVER_H
