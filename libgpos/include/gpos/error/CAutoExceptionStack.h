//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CAutoExceptionStack.h
//
//	@doc:
//		Auto object for saving and restoring exception stack
//
//	@owner:
//		elhela
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoExceptionStack_H
#define GPOS_CAutoExceptionStack_H

#include "gpos/base.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoExceptionStack
	//
	//	@doc:
    //		Auto object for saving and restoring exception stack
	//
	//---------------------------------------------------------------------------
	class CAutoExceptionStack : public CStackObject
	{

		private:

			// address of the global exception stack value
			void **m_ppvExceptionStack;

			// value of exception stack when object is created
			void *m_pvExceptionStack;

			// address of the global error context stack value
			void **m_ppvErrorContextStack;

			// value of error context stack when object is created
			void *m_pvErrorContextStack;

			// private copy ctor
			CAutoExceptionStack(const CAutoExceptionStack &);

		public:

			// ctor
			CAutoExceptionStack(void **ppvExceptionStack, void **ppvErrorContextStack);

			// dtor
			~CAutoExceptionStack();

			// set the exception stack to the given address
			void SetLocalJmp(void *pvLocalJmp);

	}; // class CAutoExceptionStack
}

#endif // !GPOS_CAutoExceptionStack_H

// EOF

