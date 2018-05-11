//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWorkerId.h
//
//	@doc:
//		Abstraction of worker identification
//---------------------------------------------------------------------------
#ifndef GPOS_CWorkerId_H
#define GPOS_CWorkerId_H

#include "gpos/types.h"
#include "gpos/common/pthreadwrapper.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CWorkerId
	//
	//	@doc:
	//		identification class; flat, i.e. doesn't need copy ctor
	//
	//---------------------------------------------------------------------------
	class CWorkerId
	{	
		private:
		
			// id of thread
			PTHREAD_T m_pthread;
		
		public:
		
			// ctor
			CWorkerId(BOOL valid = true);
			
			// simple comparison
			BOOL Equals(const CWorkerId &wid) const;

			// set worker id to current thread
			void SetThreadToCurrent();

			// reset to invalid id
			void SetThreadToInvalid();

#ifdef GPOS_DEBUG
			// check if worker id is valid
			BOOL IsValid() const;
#endif // GPOS_DEBUG

			// comparison operator
			inline
			BOOL operator == 
				(
				const CWorkerId &wid
				)
				const
			{
				return this->Equals(wid);
			}

			// comparison function; used in hashtables
			static
			BOOL Equals
				(
				const CWorkerId &wid,
				const CWorkerId &wid_other
				)
			{
				return wid == wid_other;
			}

			// primitive hash function
			static 
			ULONG HashValue(const CWorkerId &wid);

			// invalid worker id
			static
			const CWorkerId m_wid_invalid;

	}; // class CWorkerId
}

#endif // !GPOS_CWorkerId_H

// EOF

