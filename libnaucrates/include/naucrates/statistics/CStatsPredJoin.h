//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CStatsPredJoin.h
//
//	@doc:
//		Join predicate used for join cardinality estimation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredJoin_H
#define GPNAUCRATES_CStatsPredJoin_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "naucrates/statistics/CStatsPred.h"
#include "naucrates/md/IMDType.h"

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPredJoin
	//
	//	@doc:
	//		Join predicate used for join cardinality estimation
	//---------------------------------------------------------------------------
	class CStatsPredJoin : public CRefCount
	{
		private:

			// private copy ctor
			CStatsPredJoin(const CStatsPredJoin &);

			// private assignment operator
			CStatsPredJoin& operator=(CStatsPredJoin &);

			// column id
			ULONG m_ulColId1;

			// comparison type
			CStatsPred::EStatsCmpType m_escmpt;

			// column id
			ULONG m_ulColId2;

		public:

			// c'tor
			CStatsPredJoin
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
			~CStatsPredJoin()
			{}

	}; // class CStatsPredJoin

	// array of filters
	typedef CDynamicPtrArray<CStatsPredJoin, CleanupRelease> DrgPstatspredjoin;
}

#endif // !GPNAUCRATES_CStatsPredJoin_H

// EOF
