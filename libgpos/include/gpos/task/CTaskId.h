//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTaskId.h
//
//	@doc:
//		Abstraction of task identification
//---------------------------------------------------------------------------

#ifndef GPOS_CTaskId_H
#define GPOS_CTaskId_H

#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/sync/CAtomicCounter.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CTaskId
	//
	//	@doc:
	//		Identification class; uses a serial number.
	//
	//---------------------------------------------------------------------------
	class CTaskId
	{

		private:

			// task id
			ULONG_PTR m_ulptid;

			// atomic counter
			static CAtomicULONG_PTR m_aupl;

		public:

			// ctor
			CTaskId()
				:
				m_ulptid(m_aupl.TIncr())
			{}

			// simple comparison
			BOOL FEqual
				(
				const CTaskId &tid
				)
				const
			{
				return m_ulptid == tid.m_ulptid;
			}

			// comparison operator
			inline
			BOOL operator ==
				(
				const CTaskId &tid
				)
				const
			{
				return this->FEqual(tid);
			}

			// comparison function; used in hashtables
			static
			BOOL FEqual
				(
				const CTaskId &tid,
				const CTaskId &tidOther
				)
			{
				return tid == tidOther;
			}

			// primitive hash function
			static
			ULONG UlHash(const CTaskId &tid)
			{
				return gpos::UlHash<ULONG_PTR>(&tid.m_ulptid);
			}

			// invalid id
			static
			const CTaskId m_tidInvalid;

	}; // class CTaskId

}

#endif // !GPOS_CTaskId_H

// EOF

