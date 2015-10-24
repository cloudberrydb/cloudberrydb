/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_Relation_h
#define __pljava_Relation_h

#include "pljava/type/Type.h"
#ifdef __cplusplus
extern "C" {
#endif

#include <utils/rel.h>

/*******************************************************************
 * The Relation java class extends the NativeStruct and provides JNI
 * access to some of the attributes of the Relation structure.
 * 
 * @author Thomas Hallgren
 *******************************************************************/

/*
 * Create an instance of org.postgresql.pljava.Relation
 */
extern jobject Relation_create(Relation rel);

#ifdef __cplusplus
}
#endif
#endif
