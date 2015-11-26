//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		base.h
//
//	@doc:
//		Collection of commonly used OS abstraction primitives;
//		Most files should be fine including only this one from the GPOS folder;
//
//	@owner: 
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_base_H
#define GPOS_base_H

#define ALLOW_memcpy

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/error/CException.h"
#include "gpos/error/CErrorHandler.h"
#include "gpos/error/IErrorContext.h"
#include "gpos/error/ILogger.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/task/ITask.h"
#include "gpos/task/IWorker.h"

#include "gpos/common/banned_posix_nonreentrant.h"
#include "gpos/common/banned_posix_reentrant.h"
#include "gpos/common/banned_gpdb.h"

#endif // GPOS_base_H

// EOF

