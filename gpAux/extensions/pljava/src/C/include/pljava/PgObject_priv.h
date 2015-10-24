/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_PgObject_priv_h
#define __pljava_PgObject_priv_h

#include "pljava/PgObject.h"

#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************
 * Private section of the PgObject class
 *
 * @author Thomas Hallgren
 *****************************************************************/

/*
 * In C++, this would be known as the virtual destructor.
 */
typedef void (*Finalizer) (PgObject self);

/*
 * C++ has no instantiated class. This is comparable to
 * the virtual function table (vtbl) but it includes some
 * stuff in addition to functions.
 */
struct PgObjectClass_
{
	/*
	 * Allocated size for instances of this class.
	 */
	Size instanceSize;

	/*
	 * Name of class.
	 */
	const char* name;

	/*
	 * "virtual" finalizer.
	 */
	Finalizer finalize;
};

struct PgObject_
{
	PgObjectClass m_class;
};

/*
 * Internal bogus. Someone forgot to replace a function
 * pointer somewhere.
 */
extern void _PgObject_pureVirtualCalled(PgObject self);

/*
 * Throw an exception indicating that wanted member could not be
 * found.
 */
extern void PgObject_throwMemberError(jclass cls, const char* memberName, const char* signature, bool isMethod, bool isStatic);

/*
 * Allocate an instance in the given MemoryContext.
 */
extern PgObject PgObjectClass_allocInstance(PgObjectClass clazz, MemoryContext ctx);

/*
 * Initialize a class instance with given name, instance size, and finalizer. If
 * the finalizer is NULL, the default PgObject_finalize (which does nothing)
 * will be used.
 */
extern void PgObjectClass_init(PgObjectClass self, const char* name, Size instanceSize, Finalizer finalizer);

/*
 * Create a default class instance with given name and finalizer.
 */
extern PgObjectClass PgObjectClass_create(const char* name, Size instanceSize, Finalizer finalizer);

#ifdef __cplusplus
}
#endif
#endif
