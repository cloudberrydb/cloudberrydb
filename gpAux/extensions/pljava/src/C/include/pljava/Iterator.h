/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_Iterator_h
#define __pljava_Iterator_h

#include "pljava/HashMap.h"

#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************
 * An Iterator that backed by the given HashMap. The
 * Iterator will indicate no more entries if the HashMap grows
 * so that it needs to rehash.
 * 
 * The Iterator is allocated using the same MemoryContext
 * as the HashMap.
 * 
 * @author Thomas Hallgren
 *
 ***********************************************************************/

/*
 * Creates an Iterator.
 */
extern Iterator Iterator_create(HashMap source);

/*
 * Return true if the Iterator has more entries.
 */
extern bool Iterator_hasNext(Iterator self);

/*
 * Return the next Entry from the backing HashMap or NULL when
 * no more entries exists.
 */
extern Entry Iterator_next(Iterator self);

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
