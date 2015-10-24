/*-------------------------------------------------------------------------
 *
 * aocatalog.h
 *
 * Helper function to support the creation of
 * append-only auxiliary relation as block directories and visimaps.
 *
 * Copyright (c) 2013, Pivotal Inc.
 *-------------------------------------------------------------------------
 */
#ifndef AOCATALOG_H
#define AOCATALOG_H

#include "postgres.h"
#include "catalog/heap.h"
#include "catalog/index.h"

bool CreateAOAuxiliaryTable(
		Relation rel,
		const char *auxiliaryNamePrefix,
		char relkind,
		Oid aoauxiliaryOid,
		Oid aoauxiliaryIndexOid,
		Oid *aoauxiliaryComptypeOid,
		TupleDesc tupledesc,
		IndexInfo  *indexInfo,
		Oid	*classObjectId);

#endif   /* AOCATALOG_H */
