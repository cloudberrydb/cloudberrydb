/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_Backend_h
#define __pljava_Backend_h

#include "pljava/Function.h"

#ifdef __cplusplus
extern "C" {
#endif

/*****************************************************************
 * The Backend contains the call handler, initialization of the
 * PL/Java, access to config variables, and logging.
 * 
 * @author Thomas Hallgren
 *****************************************************************/
extern bool integerDateTimes;

void Backend_setJavaSecurity(bool trusted);

int Backend_setJavaLogLevel(int logLevel);

#ifdef __cplusplus
}
#endif

#endif /* !__pljava_Backend_h */
