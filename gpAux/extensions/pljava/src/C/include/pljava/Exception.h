/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_Exception_h
#define __pljava_Exception_h

#include "pljava/PgObject.h"

#ifdef __cplusplus
extern "C" {
#endif

/*******************************************************************
 * Java exception related things
 * 
 * @author Thomas Hallgren
 *
 *******************************************************************/

/*
 * Trows an UnsupportedOperationException informing the caller that the
 * requested feature doesn't exist in the current version, it was introduced
 * starting with the intro version.
 */
extern void	Exception_featureNotSupported(const char* requestedFeature, const char* introVersion);

/*
 * Like ereport(ERROR, ...) but this method will raise a Java SQLException and
 * return. It will NOT do a longjmp. Suitable in native code that is called
 * from Java (such code must return to Java in order to have the real exception
 * thrown).
 */
extern void Exception_throw(int errCode, const char* errMessage, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 2, 3)));

/*
 * Like ereport(ERROR, ...) but this method will raise a Java IllegalArgumentException and
 * return. It will NOT do a longjmp. Suitable in native code that is called
 * from Java (such code must return to Java in order to have the real exception
 * thrown).
 */
extern void Exception_throwIllegalArgument(const char* errMessage, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

/*
 * Like ereport(ERROR, ...) but this method will raise a Java SQLException and
 * return. It will NOT do a longjmp.
 */
extern void Exception_throwSPI(const char* function, int errCode);

/*
 * This method will raise a Java ServerException based on an ErrorData obtained
 * by a call to CopyErrorData. It will NOT do a longjmp. It's intended use is
 * in PG_CATCH clauses.
 */
extern void Exception_throw_ERROR(const char* function);

/*
 * Throw an exception indicating that wanted member could not be
 * found. This is an ereport(ERROR...) so theres' no return from
 * this function.
 */
extern void Exception_throwMemberError(const char* memberName, const char* signature, bool isMethod, bool isStatic);

#ifdef __cplusplus
}
#endif
#endif
