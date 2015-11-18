//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolStatistics.h
//
//	@doc:
//		Statistics for memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolStatistics_H
#define GPOS_CMemoryPoolStatistics_H

#include "gpos/types.h"


namespace gpos
{
	// Statistics for a memory pool
	class CMemoryPoolStatistics
	{
		private:

			ULLONG m_ullSuccessfulAllocations;

			ULLONG m_ullFailedAllocations;

			ULLONG m_ullFree;

			ULLONG m_ullLiveObj;

			ULLONG m_ullLiveObjUserSize;

			ULLONG m_ullLiveObjTotalSize;

			// private copy ctor
			CMemoryPoolStatistics(CMemoryPoolStatistics &);

		public:

			// ctor
			CMemoryPoolStatistics()
				:
				m_ullSuccessfulAllocations(0),
				m_ullFailedAllocations(0),
				m_ullFree(0),
				m_ullLiveObj(0),
				m_ullLiveObjUserSize(0),
				m_ullLiveObjTotalSize(0)
			 {}

			// dtor
			virtual ~CMemoryPoolStatistics()
			{}

			// get the total number of successful allocation calls
			ULLONG UllSuccessfulAllocations() const
			{
				return m_ullSuccessfulAllocations;
			}

			// get the total number of failed allocation calls
			ULLONG UllFailedAllocations() const
			{
				return m_ullFailedAllocations;
			}

			// get the total number of free calls
			ULLONG UllFree() const
			{
				return m_ullFree;
			}

			// get the number of live objects
			ULLONG UllLiveObj() const
			{
				return m_ullLiveObj;
			}

			// get the user data size of live objects
			ULLONG UllLiveObjUserSize() const
			{
				return m_ullLiveObjUserSize;
			}

			// get the total data size (user + header padding) of live objects;
			// not accounting for memory used by the underlying allocator for its header;
			ULLONG UllLiveObjTotalSize() const
			{
				return m_ullLiveObjTotalSize;
			}

			// record a successful allocation
			void RecordAllocation
				(
				ULONG ulUserDataSize,
				ULONG ulTotalDataSize
				)
			{
				++m_ullSuccessfulAllocations;
				++m_ullLiveObj;
				m_ullLiveObjUserSize += ulUserDataSize;
				m_ullLiveObjTotalSize += ulTotalDataSize;
			}

			// record a successful free call (of a valid, non-NULL pointer)
			void RecordFree
				(
				ULONG ulUserDataSize,
				ULONG ulTotalDataSize
				)
			{
				++m_ullFree;
				--m_ullLiveObj;
				m_ullLiveObjUserSize -= ulUserDataSize;
				m_ullLiveObjTotalSize -= ulTotalDataSize;
			}

			// record a failed allocation attempt
			void RecordFailedAllocation()
			{
				++m_ullFailedAllocations;
			}

			// return total allocated size
			virtual
			ULLONG UllTotalAllocatedSize() const
			{
				return m_ullLiveObjTotalSize;
			}

	}; // class CMemoryPoolStatistics
}

#endif // ! CMemoryPoolStatistics

// EOF

