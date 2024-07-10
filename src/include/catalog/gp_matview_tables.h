/*-------------------------------------------------------------------------
 *
 * gp_matview_tables.h
 *	  Definitions for the gp_matview_tables catalog table.
 *	  This is used to record the relations between
 *	  materialized view and base tables.
 *
 * Portions Copyright (c) 2024-Present HashData, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/gp_matview_tables.h
 *
 * NOTES
 * 		This should cooperate with gp_matview_aux.
 *
 *-------------------------------------------------------------------------
 */

#ifndef gp_matview_tables_H
#define gp_matview_tables_H

#include "catalog/genbki.h"
#include "catalog/gp_matview_tables_d.h"

/*
 * Defines for gp_matview_tables
 */
CATALOG(gp_matview_tables,7150,GpMatviewTablesId) BKI_SHARED_RELATION
{
	Oid			mvoid; 	/* materialized view oid */
	Oid			relid; 	/* base table oid */
} FormData_gp_matview_tables;


/* ----------------
 *		Form_gp_matview_tables corresponds to a pointer to a tuple with
 *		the format of gp_matview_tables relation.
 * ----------------
 */
typedef FormData_gp_matview_tables *Form_gp_matview_tables;

DECLARE_UNIQUE_INDEX(gp_matview_tables_mvoid_relid_index, 7151, on gp_matview_tables using btree(mvoid oid_ops, relid oid_ops));
#define GpMatviewTablesMvRelIndexId 7151

DECLARE_INDEX(gp_matview_tables_relid_index, 7152, on gp_matview_tables using btree(relid oid_ops));
#define GpMatviewTablesRelIndexId 7152

#endif			/* gp_matview_tables_H */
