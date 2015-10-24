/*-------------------------------------------------------------------------
 *
 * pg_foreign_server.h
 *	  definition of the system "foreign server" relation (pg_foreign_server)
 *
 * Copyright (c) 2009-2010, Greenplum Inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/catalog/pg_foreign_server.h,v 1.3 2009/06/11 14:49:09 momjian Exp $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_FOREIGN_SERVER_H
#define PG_FOREIGN_SERVER_H

#include "catalog/genbki.h"

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE pg_foreign_server
   with (camelcase=ForeignServer, relid=2899, reltype_oid=6448)
   (
   srvname       name, -- foreign server name
   srvowner      oid, -- server owner
   srvfdw      oid, -- server FDW

   -- VARIABLE LENGTH FIELDS start here.  These fields may be NULL, too.

   srvtype     text,
   srvversion  text,
   srvacl        aclitem[], -- access permissions
   srvoptions    text[] -- FDW-specific options
   );

   create unique index on pg_foreign_server(oid) with (indexid=3308, CamelCase=ForeignServerOid, syscacheid=FOREIGNSERVEROID, syscache_nbuckets=32);
   create unique index on pg_foreign_server(srvname) with (indexid=3309, CamelCase=ForeignServerName, syscacheid=FOREIGNSERVERNAME, syscache_nbuckets=32);

   alter table pg_foreign_server add fk srvowner on pg_authid(oid);
   alter table pg_foreign_server add fk srvfdw on pg_foreign_data_wrapper(oid);

   TIDYCAT_ENDFAKEDEF
*/

/* ----------------
 *		pg_foreign_server definition.  cpp turns this into
 *		typedef struct FormData_pg_foreign_server
 * ----------------
 */
#define ForeignServerRelationId 2899

CATALOG(pg_foreign_server,2899)
{
	NameData	srvname;		/* foreign server name */
	Oid			srvowner;		/* server owner */
	Oid			srvfdw;			/* server FDW */

	/*
	 * VARIABLE LENGTH FIELDS start here.  These fields may be NULL, too.
	 */
	text		srvtype;
	text		srvversion;
	aclitem		srvacl[1];		/* access permissions */
	text		srvoptions[1];	/* FDW-specific options */
} FormData_pg_foreign_server;

/* ----------------
 *		Form_pg_foreign_server corresponds to a pointer to a tuple with
 *		the format of pg_foreign_server relation.
 * ----------------
 */
typedef FormData_pg_foreign_server *Form_pg_foreign_server;

/* ----------------
 *		compiler constants for pg_foreign_server
 * ----------------
 */

#define Natts_pg_foreign_server					7
#define Anum_pg_foreign_server_srvname			1
#define Anum_pg_foreign_server_srvowner			2
#define Anum_pg_foreign_server_srvfdw			3
#define Anum_pg_foreign_server_srvtype			4
#define Anum_pg_foreign_server_srvversion		5
#define Anum_pg_foreign_server_srvacl			6
#define Anum_pg_foreign_server_srvoptions		7

#endif   /* PG_FOREIGN_SERVER_H */
