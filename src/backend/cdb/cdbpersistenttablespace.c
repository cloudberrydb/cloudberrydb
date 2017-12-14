/*-------------------------------------------------------------------------
 *
 * cdbpersistenttablespace.c
 *
 * Portions Copyright (c) 2009-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbpersistenttablespace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "access/persistentfilesysobjname.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_filespace.h"
#include "cdb/cdbsharedoidsearch.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistenttablespace.h"
#include "postmaster/postmaster.h"
#include "storage/itemptr.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "storage/smgr.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"

PersistentTablespaceGetFilespaces
PersistentTablespace_TryGetPrimaryAndMirrorFilespaces(
													  Oid tablespaceOid,
 /* The tablespace OID for the create. */

													  char **primaryFilespaceLocation,
 /* The primary filespace directory path.  Return NULL for global and base. */

													  char **mirrorFilespaceLocation,

 /*
  * The primary filespace directory path.  Return NULL for global and base.
  * Or, returns NULL when mirror not configured.
  */

													  Oid *filespaceOid)
{
	*primaryFilespaceLocation = NULL;
	*mirrorFilespaceLocation = NULL;
	*filespaceOid = InvalidOid;

	// WALREP_FIXME: Heikki: I stubbed out this function to just always return a
	// dummy location. What should we do here?
	if (tablespaceOid > FirstNormalObjectId)
			*primaryFilespaceLocation = psprintf("tblspc_fixme_%u", tablespaceOid);

	/*
	 * Optimize out the common cases.
	 */
	return PersistentTablespaceGetFilespaces_Ok;
}

void
PersistentTablespace_GetPrimaryAndMirrorFilespaces(
												   Oid tablespaceOid,
 /* The tablespace OID for the create. */

												   char **primaryFilespaceLocation,
 /* The primary filespace directory path.  Return NULL for global and base. */

												   char **mirrorFilespaceLocation)

 /*
  * The primary filespace directory path.  Return NULL for global and base.
  * Or, returns NULL when mirror not configured.
  */
{
	PersistentTablespaceGetFilespaces tablespaceGetFilespaces;

	Oid			filespaceOid;

	/*
	 * Do not call PersistentTablepace_VerifyInitScan here to allow
	 * PersistentTablespace_TryGetPrimaryAndMirrorFilespaces to handle the
	 * Standby Master special case.
	 */

	tablespaceGetFilespaces =
		PersistentTablespace_TryGetPrimaryAndMirrorFilespaces(
															  tablespaceOid,
															  primaryFilespaceLocation,
															  mirrorFilespaceLocation,
															  &filespaceOid);
	switch (tablespaceGetFilespaces)
	{
		case PersistentTablespaceGetFilespaces_TablespaceNotFound:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unable to find entry for tablespace OID = %u when getting filespace directory paths",
							tablespaceOid)));
			break;

		case PersistentTablespaceGetFilespaces_FilespaceNotFound:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unable to find entry for filespace OID = %u when forming filespace directory paths for tablespace OID = %u",
							filespaceOid,
							tablespaceOid)));
			break;

		case PersistentTablespaceGetFilespaces_Ok:
			/* Go below and pass back the result. */
			break;

		default:
			elog(ERROR, "Unexpected tablespace filespace fetch result: %d",
				 tablespaceGetFilespaces);
	}
}
