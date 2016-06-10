//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CStatisticsJoin.h
//
//	@doc:
//		Join on statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatisticsJoin_H
#define GPNAUCRATES_CStatisticsJoin_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/md/IMDType.h"

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatisticsJoin
	//
	//	@doc:
	//		Join on statistics
	//---------------------------------------------------------------------------
	class CStatisticsJoin : public CRefCount
	{
		private:

			// private copy ctor
			CStatisticsJoin(const CStatisticsJoin &);

			// private assignment operator
			CStatisticsJoin& operator=(CStatisticsJoin &);

			// column id
			ULONG m_ulColId1;

			// comparison type
			CStatsPred::EStatsCmpType m_escmpt;

			// column id
			ULONG m_ulColId2;

		public:

			// c'tor
			CStatisticsJoin
				(
				ULONG ulColId1,
				CStatsPred::EStatsCmpType escmpt,
				ULONG ulColId2
				)
				:
				m_ulColId1(ulColId1),
				m_escmpt(escmpt),
				m_ulColId2(ulColId2)
			{}

			// accessors
			ULONG UlColId1() const
			{
				return m_ulColId1;
			}

			// comparison type
			CStatsPred::EStatsCmpType Escmpt() const
			{
				return m_escmpt;
			}

			ULONG UlColId2() const
			{
				return m_ulColId2;
			}

			// d'tor
			virtual
			~CStatisticsJoin()
			{}

	}; // class CStatisticsJoin

	// array of filters
	typedef CDynamicPtrArray<CStatisticsJoin, CleanupRelease> DrgPstatsjoin;
}

#endif // !GPNAUCRATES_CStatisticsJoin_H

// EOF
