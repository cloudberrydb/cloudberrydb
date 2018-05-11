//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPred.h
//
//	@doc:
//		Filter on statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPred_H
#define GPNAUCRATES_CStatsPred_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPred
	//
	//	@doc:
	//		Filter on statistics
	//---------------------------------------------------------------------------
	class CStatsPred : public CRefCount
	{
		public:

			enum EStatsPredType
			{
				EsptPoint, // filter with literals
				EsptConj, // conjunctive filter
				EsptDisj, // disjunctive filter
				EsptLike, // LIKE filter
				EsptUnsupported, // unsupported filter for statistics calculation

				EsptSentinel
			};

			// comparison types for stats computation
			enum EStatsCmpType
			{
				EstatscmptEq,	// equals
				EstatscmptNEq,	// not equals
				EstatscmptL,		// less than
				EstatscmptLEq,	// less or equal to
				EstatscmptG,		// greater than
				EstatscmptGEq,	// greater or equal to
				EstatscmptIDF,	// is distinct from
				EstatscmptINDF,	// is not distinct from
				EstatscmptLike,	// LIKE predicate comparison
				EstatscmptNotLike,	// NOT LIKE predicate comparison
				EstatscmptEqNDV, // NDV comparision for equality predicate on columns with functions, ex f(a) = b or f(a) = f(b)

				EstatscmptOther
			};

		private:

			// private copy ctor
			CStatsPred(const CStatsPred &);

			// private assignment operator
			CStatsPred& operator=(CStatsPred &);

		protected:

			// column id
			ULONG m_ulColId;

		public:

			// ctor
			explicit
			CStatsPred
				(
				ULONG ulColId
				)
				:
				m_ulColId(ulColId)
			{
			}

			// dtor
			virtual
			~CStatsPred()
			{
			}

			// accessors
			virtual
			ULONG UlColId() const
			{
				return m_ulColId;
			}

			// type id
			virtual
			EStatsPredType Espt() const = 0;

			// comparison function
			static
			inline INT IStatsFilterCmp(const void *pv1, const void *pv2);
	}; // class CStatsPred

	// array of filters
	typedef CDynamicPtrArray<CStatsPred, CleanupRelease> DrgPstatspred;

	// comparison function for sorting filters
	INT CStatsPred::IStatsFilterCmp
		(
		const void *pv1,
		const void *pv2
		)
	{
		const CStatsPred *pstatspred1 = *(const CStatsPred **) pv1;
		const CStatsPred *pstatspred2 = *(const CStatsPred **) pv2;

		return (INT) pstatspred1->UlColId() - (INT) pstatspred2->UlColId();
	}

}

#endif // !GPNAUCRATES_CStatsPred_H

// EOF
