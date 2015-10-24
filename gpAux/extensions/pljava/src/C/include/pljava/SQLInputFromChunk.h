/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_SQLInputFromChunk_h
#define __pljava_SQLInputFromChunk_h

#include "pljava/PgObject.h"

#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************
 * Provides mapping between java.sql.SQLInput and a chunk of memory
 * allocated by the backend.
 * 
 * @author Thomas Hallgren
 *
 ***********************************************************************/

jobject SQLInputFromChunk_create(void* data, size_t dataSize);
void SQLInputFromChunk_close(jobject input);

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
