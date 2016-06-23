/*-------------------------------------------------------------------------
 *
 * cdbdisp_gang.h
 * routines for multi-thread implementation of creating gang
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBGANG_THREAD_H
#define CDBGANG_THREAD_H

#include "cdb/cdbgang.h"

extern CreateGangFunc pCreateGangFuncThreaded;

#endif
