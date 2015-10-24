/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_Oid_h
#define __pljava_Oid_h

#include "pljava/type/Type.h"
#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************
 * The Oid java class represents the native Oid.
 * 
 * @author Thomas Hallgren
 *****************************************************************/

/*
 * Create the org.postgresql.pljava.Oid instance
 */
extern jobject Oid_create(Oid oid);

/*
 * Extract the native Oid from a Java Oid.
 */
extern Oid Oid_getOid(jobject joid);

/*
 * Map a java.sql.Types SQL type to an Oid.
 */
extern Oid Oid_forSqlType(int sqlType);

#ifdef __cplusplus
}
#endif
#endif
