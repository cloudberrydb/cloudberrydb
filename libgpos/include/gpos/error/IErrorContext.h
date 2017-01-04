//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IErrorContext.h
//
//	@doc:
//		Interface for error context to record error message, stack, etc.
//---------------------------------------------------------------------------
#ifndef GPOS_IErrorContext_H
#define GPOS_IErrorContext_H

#include "gpos/types.h"
#include "gpos/error/CException.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		IErrorContext
	//
	//	@doc:
	//		Abstraction for context object, owned by Task
	//
	//---------------------------------------------------------------------------
	class IErrorContext
	{

		private:

			// private copy ctor
			IErrorContext(const IErrorContext&);

		public:

			// ctor
			IErrorContext()
			{}
			
			// dtor
			virtual
			~IErrorContext()
			{}

			// reset context, clear out handled error
			virtual
			void Reset() = 0;
			
			// record error context
			virtual
			void Record(CException &exc, VA_LIST) = 0;
		
			// exception accessor
			virtual
			CException Exc() const = 0;

			// error message accessor
			virtual
			const WCHAR *WszMsg() const = 0;
			
			// copy necessary info for error propagation
			virtual
			void CopyPropErrCtxt(const IErrorContext *perrctxt) = 0;

			// severity accessor
			virtual
			ULONG UlSev() const = 0;

			// set severity
			virtual
			void SetSev(ULONG ulSev) = 0;

			// print error stack trace
			virtual
			void AppendStackTrace() = 0;

			// print errno message
			virtual
			void AppendErrnoMsg() = 0;

			// check if there is a pending exception
			virtual
			BOOL FPending() const = 0;

			// check if exception is rethrown
			virtual
			BOOL FRethrow() const = 0;

			// mark that exception is rethrown
			virtual
			void SetRethrow() = 0;

	}; // class IErrorContext
}

#endif // !GPOS_IErrorContext_H

// EOF

