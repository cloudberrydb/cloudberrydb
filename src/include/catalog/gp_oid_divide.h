/*-------------------------------------------------------------------------
 *
 * oid_divide.h
 * 
 * Portions Copyright (c) 2024-Present HashData, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/oid_divide.h
 *
 * NOTES
 * 		This is used to divide oid range for core and extensions
 *  	to avoid duplicated.
 *
 *-------------------------------------------------------------------------
 */

#ifndef GP_OID_DIVIDE_H
#define GP_OID_DIVIDE_H

/*
 * Extensions should use Oids start from EXT_OID_START!
 *
 * To avoid duplicated oids across extensions or repos.
 * We strongly suggest extesions should begin from EXT_OID_START
 * to separate kernel and extensions.
 */
#define EXT_OID_START 9932

#endif			/* GP_OID_DIVIDE_H */
