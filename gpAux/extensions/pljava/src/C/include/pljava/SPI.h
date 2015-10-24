/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_SPI_h
#define __pljava_SPI_h

#include "pljava/PgObject.h"

#include <executor/spi.h>

#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************
 * Some needed additions to the SPI set of functions.
 * 
 * @author Thomas Hallgren
 *
 ***********************************************************************/

typedef struct
{
	SubTransactionId xid;
	int  nestingLevel;
	char name[1];
} Savepoint;

/* infant is set to the savepoint that is being created durin a setSavepoint call.
 * It is used by the onStart callback.
 */
extern Savepoint* infant;

extern Savepoint* SPI_setSavepoint(const char* name);

extern void SPI_releaseSavepoint(Savepoint* sp);

extern void SPI_rollbackSavepoint(Savepoint* sp);

#ifdef __cplusplus
} /* end of extern "C" declaration */
#endif
#endif
