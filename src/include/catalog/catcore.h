/*-------------------------------------------------------------------------
 *
 * catcore.h
 *	  declarations for catcore access methods
 *
 * CatCore is for general use, but currently resides under ucs because
 * it is the only client that uses CatCore.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CATCORE_H
#define CATCORE_H

#include "postgres.h"
#include "access/attnum.h"

/* When change this, you should change it in generator script, too. */
#define MAX_SCAN_NUM 4

/*
 * Type in the catalog.
 * We denormalize btree procedures here for efficiency.
 */
typedef struct CatCoreType
{
	Oid			typid;			/* oid of this type */
	Oid			eqfunc;			/* function for BTEqualStrategyNumber */
	Oid			ltfunc;			/* function for BTLessStrategyNumber */
	Oid			lefunc;			/* function for BTLessEqualStrategyNumber */
	Oid			gefunc;			/* function for BTGreaterEqualStrategyNumber */
	Oid			gtfunc;			/* function for BTGreaterStrategyNumber */
} CatCoreType;

/*
 * Attribute in the catalog.
 */
typedef struct CatCoreAttr
{
	const char *attname;		/* attribute name */
	AttrNumber	attnum;			/* attribute number */
	const CatCoreType *atttyp;		/* attribute type */
} CatCoreAttr;

/*
 * Index in the catalog.
 * We use fixed-length array for key attributes because we know its maximum
 * length and this way is more compact than having separate pointers.
 */
typedef struct CatCoreIndex
{
	Oid			indexoid;		/* oid of this index relation */
	/* key attributes.  filled with InvalidOid */
	AttrNumber	attnums[MAX_SCAN_NUM];
	int			nkeys;			/* number of valid attributes in attnums */
} CatCoreIndex;

/*
 * Relation in the catalog.
 * This is only for tables, and index relations are not included.
 */
typedef struct CatCoreRelation
{
	const char *relname;		/* relation name */
	Oid			relid;			/* oid of this relation */
	const CatCoreAttr *attributes;	/* pointer to attribute array */
	int			natts;			/* number of attributes */
	const CatCoreIndex *indexes;/* pointer to index array */
	int			nindexes;		/* number of indexes */
	bool		hasoid;			/* true if this table has oid column */
} CatCoreRelation;

/*
 * catcoretable.c
 * These are generated from catcore json.
 */
extern const CatCoreAttr TableOidAttr;
extern const CatCoreRelation CatCoreRelations[];
extern const int CatCoreRelationSize;

/* catcore.c */
extern const CatCoreRelation *catcore_lookup_rel(char *name);
extern AttrNumber catcore_lookup_attnum(const CatCoreRelation *relation,
					  char *attname, Oid *atttype);
extern const CatCoreAttr *catcore_lookup_attr(const CatCoreRelation *relation,
			char *attname);
extern const CatCoreType *catcore_lookup_type(Oid typid);


#endif
