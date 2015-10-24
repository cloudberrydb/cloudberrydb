/*-------------------------------------------------------------------------
 *
 * pg_proc_callback.c
 *	  
 *   Auxillary extension to pg_proc to enable defining additional callback
 *   functions.  Currently the list of allowable callback functions is small
 *   and consists of:
 *     - DESCRIBE() - to support more complex pseudotype resolution
 *
 * Copyright (c) EMC, 2011
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc_callback.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"

/* ---------------------
 * deleteProcCallbacks() - Remove callbacks from pg_proc_callback
 *
 * Parameters:
 *    profnoid - remove all callbacks for this function oid
 *
 * Notes:
 *    This function does not maintain dependencies in pg_depend, that behavior
 *    is currently controlled in pg_proc.c
 * ---------------------
 */
void 
deleteProcCallbacks(Oid profnoid)
{
	Insist(OidIsValid(profnoid));

	/* 
	 * Boiler template code to loop through the index and remove all matching
	 * rows.
	 */

	(void) caql_getcount(
		NULL,
		cql("DELETE FROM pg_proc_callback "
			" WHERE profnoid = :1 ",
			ObjectIdGetDatum(profnoid)));
}


/* ---------------------
 * addProcCallback() - Add a new callback to pg_proc_callback
 *
 * Parameters:
 *    profnoid    - oid of the function that has a callback
 *    procallback - oid of the callback function
 *    promethod   - role the callback function plays
 *
 * Notes:
 *    This function does not maintain dependencies in pg_depend, that behavior
 *    is currently controlled in pg_proc.c
 * ---------------------
 */
void 
addProcCallback(Oid profnoid, Oid procallback, char promethod)
{
	bool		nulls[Natts_pg_proc_callback];
	Datum		values[Natts_pg_proc_callback];
	HeapTuple   tup;
	cqContext  *pcqCtx;
	
	Insist(OidIsValid(profnoid));
	Insist(OidIsValid(procallback));

	/* open pg_proc_callback */
	pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_proc_callback ",
				NULL));

	/* Build the tuple and insert it */
	nulls[Anum_pg_proc_callback_profnoid - 1]	  = false;
	nulls[Anum_pg_proc_callback_procallback - 1]  = false;
	nulls[Anum_pg_proc_callback_promethod - 1]	  = false;
	values[Anum_pg_proc_callback_profnoid - 1]	  = ObjectIdGetDatum(profnoid);
	values[Anum_pg_proc_callback_procallback - 1] = ObjectIdGetDatum(procallback);
	values[Anum_pg_proc_callback_promethod - 1]	  = CharGetDatum(promethod);

	tup = caql_form_tuple(pcqCtx, values, nulls);
	
	/* Insert tuple into the relation */
	caql_insert(pcqCtx, tup);  /* implicit update of index as well */

	caql_endscan(pcqCtx);
}


/* ---------------------
 * lookupProcCallback() - Find a specified callback for a specified function
 *
 * Parameters:
 *    profnoid    - oid of the function that has a callback
 *    promethod   - which callback to find
 * ---------------------
 */
Oid  
lookupProcCallback(Oid profnoid, char promethod)
{
	Oid         result = InvalidOid;

	Insist(OidIsValid(profnoid));

	/* Lookup (profnoid, promethod) from index */

	/* (profnoid, promethod) is guaranteed unique by the index */

	result = caql_getoid(
			NULL,
			cql("SELECT procallback FROM pg_proc_callback "
				" WHERE profnoid = :1 "
				" AND promethod = :2 ",
				ObjectIdGetDatum(profnoid),
				CharGetDatum(promethod)));

	return result;
}
