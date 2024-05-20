/*-------------------------------------------------------------------------
 *
 * pg_tag.h
 *	  definition of the "tag" system catalog (pg_tag)
 *
 *
 * Portions Copyright (c) 2024, Hashdata Inc.
 *
 * src/include/catalog/pg_tag.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_TAG_H
#define PG_TAG_H

#include "catalog/genbki.h"
#include "catalog/pg_tag_d.h"
#include "parser/parse_node.h"

/* ----------------
 *		pg_tag definition.  cpp turns this into
 *		typedef struct FormData_pg_tag
 *
 *		If you change the following, make sure you change the structs for
 *		system attributes in catalog/heap.c also.
 *		You may need to change catalog/genbki.pl as well.
 */
CATALOG(pg_tag,6461,TagRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(6462,TagRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid		oid		BKI_FORCE_NOT_NULL;		/* oid */
	NameData	tagname		BKI_FORCE_NOT_NULL;	/* name of tag */
	Oid		tagowner		BKI_DEFAULT(POSTGRES)	BKI_LOOKUP(pg_authid);	/* owner of tag */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text	allowed_values[1];		/* per tag allowed values */
#endif
} FormData_pg_tag;

/* ----------------
 *		Form_pg_tag corresponds to a pointer to a tuple with
 *		the format of pg_tag relation.
 * ----------------
 */
typedef FormData_pg_tag *Form_pg_tag;

DECLARE_UNIQUE_INDEX_PKEY(pg_tag_tagname_index, 6463, on pg_tag using btree(tagname name_ops));
#define TagNameIndexId  6463
DECLARE_UNIQUE_INDEX(pg_tag_oid_index, 6465, on pg_tag using btree(oid oid_ops));
#define TagOidIndexId   6465

#endif							/* PG_TAG_H */