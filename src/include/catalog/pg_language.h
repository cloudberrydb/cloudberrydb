/*-------------------------------------------------------------------------
 *
 * pg_language.h
 *	  definition of the "language" system catalog (pg_language)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_language.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LANGUAGE_H
#define PG_LANGUAGE_H

#include "catalog/genbki.h"
#include "catalog/pg_language_d.h"

/* ----------------
 *		pg_language definition.  cpp turns this into
 *		typedef struct FormData_pg_language
 * ----------------
 */
CATALOG(pg_language,2612,LanguageRelationId)
{
	Oid			oid;			/* oid */

	/* Language name */
	NameData	lanname;

	/* Language's owner */
	Oid			lanowner BKI_DEFAULT(POSTGRES) BKI_LOOKUP(pg_authid);

	/* Is a procedural language */
	bool		lanispl BKI_DEFAULT(f);

	/* PL is trusted */
	bool		lanpltrusted BKI_DEFAULT(f);

	/* Call handler, if it's a PL */
	Oid			lanplcallfoid BKI_DEFAULT(0) BKI_LOOKUP_OPT(pg_proc);

	/* Optional anonymous-block handler function */
	Oid			laninline BKI_DEFAULT(0) BKI_LOOKUP_OPT(pg_proc);

	/* Optional validation function */
	Oid			lanvalidator BKI_DEFAULT(0) BKI_LOOKUP_OPT(pg_proc);

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* Access privileges */
	aclitem		lanacl[1] BKI_DEFAULT(_null_);
#endif
} FormData_pg_language;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(lanplcallfoid REFERENCES pg_proc(oid));
FOREIGN_KEY(lanvalidator REFERENCES pg_proc(oid));
FOREIGN_KEY(lanowner REFERENCES pg_authid(oid));

/* ----------------
 *		Form_pg_language corresponds to a pointer to a tuple with
 *		the format of pg_language relation.
 * ----------------
 */
typedef FormData_pg_language *Form_pg_language;

DECLARE_TOAST(pg_language, 4157, 4158);

/* GPDB_14_MERGE_FIXME: GP7 use name pg_language_lanname_index and seems meaning less */
DECLARE_UNIQUE_INDEX(pg_language_name_index, 2681, on pg_language using btree(lanname name_ops));
#define LanguageNameIndexId  2681
DECLARE_UNIQUE_INDEX_PKEY(pg_language_oid_index, 2682, on pg_language using btree(oid oid_ops));
#define LanguageOidIndexId	2682

#endif							/* PG_LANGUAGE_H */
