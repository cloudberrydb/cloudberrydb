//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		types.h
//
//	@doc:
//		pthread type definitions for GPOS;
//---------------------------------------------------------------------------
#ifndef GPOS_pthreadtypes_H
#define GPOS_pthreadtypes_H

#include <errno.h>
#include <pthread.h>
#include <signal.h>

#include "gpos/types.h"

namespace gpos
{
	// describe the thread attributes
	typedef pthread_attr_t PTHREAD_ATTR_T;

	// describe the attributes of a thread mutex
	typedef pthread_mutexattr_t PTHREAD_MUTEXATTR_T;

	// describe a thread mutex
	typedef pthread_mutex_t PTHREAD_MUTEX_T;

	// describe a thread conditional variable
	typedef pthread_cond_t PTHREAD_COND_T;

	// describe the thread attributes
	typedef pthread_condattr_t PTHREAD_CONDATTR_T;

	// thread description record
	typedef pthread_t PTHREAD_T;

	// set of signals
	typedef sigset_t SIGSET_T;
}

#endif // !GPOS_pthreadtypes_H

// EOF

