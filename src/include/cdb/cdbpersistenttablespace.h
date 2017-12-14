/*-------------------------------------------------------------------------
 *
 * cdbpersistenttablespace.h
 *
 * Portions Copyright (c) 2009-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbpersistenttablespace.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPERSISTENTTABLESPACE_H
#define CDBPERSISTENTTABLESPACE_H

/*
 * The states of a persistent database directory object usablility.
 */
typedef enum PersistentTablespaceGetFilespaces
{
	PersistentTablespaceGetFilespaces_None= 0,
	PersistentTablespaceGetFilespaces_Ok,
	PersistentTablespaceGetFilespaces_TablespaceNotFound,
	PersistentTablespaceGetFilespaces_FilespaceNotFound,
	MaxPersistentTablespaceGetFilespaces /* must always be last */
} PersistentTablespaceGetFilespaces;

extern PersistentTablespaceGetFilespaces PersistentTablespace_TryGetPrimaryAndMirrorFilespaces(
	Oid 		tablespaceOid,
				/* The tablespace OID for the create. */

	char **primaryFilespaceLocation,
				/* The primary filespace directory path.  Return NULL for global and base. */
	
	char **mirrorFilespaceLocation,
				/* The primary filespace directory path.  Return NULL for global and base. 
				 * Or, returns NULL when mirror not configured. */
				 
	Oid *filespaceOid);

extern void PersistentTablespace_GetPrimaryAndMirrorFilespaces(
	Oid 		tablespaceOid,
				/* The tablespace OID for the create. */

	char **primaryFilespaceLocation,
				/* The primary filespace directory path.  Return NULL for global and base. */
	
	char **mirrorFilespaceLocation);
				/* The primary filespace directory path.  Return NULL for global and base. 
				 * Or, returns NULL when mirror not configured. */

#endif   /* CDBPERSISTENTTABLESPACE_H */
