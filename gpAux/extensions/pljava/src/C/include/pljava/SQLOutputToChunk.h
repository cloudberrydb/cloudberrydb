/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_SQLOutputToChunk_h
#define __pljava_SQLOutputToChunk_h

#include "pljava/PgObject.h"

#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************
 * Provides mapping between java.sql.SQLOutput and a StringInfo buffer
 * allocated by the backend.
 *
 * @author Thomas Hallgren
 *
 ***********************************************************************/
#include <lib/stringinfo.h>

jobject SQLOutputToChunk_create(StringInfo buffer);
void SQLOutputToChunk_close(jobject output);

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
