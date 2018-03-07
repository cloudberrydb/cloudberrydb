//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CRange.h
//
//	@doc:
//		Representation of a range of values
//---------------------------------------------------------------------------
#ifndef GPOPT_CRange_H
#define GPOPT_CRange_H

#include "gpos/base.h"
#include "gpos/types.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/operators/CExpression.h"

#include "naucrates/md/IMDType.h"

namespace gpnaucrates
{
	// fwd declarations
	class IDatum;
}

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	//fwd declarations
	class CExpression;
	class CRange;
	class IComparator;

	//---------------------------------------------------------------------------
	//	@class:
	//		CRange
	//
	//	@doc:
	//		Representation of a range of values
	//
	//---------------------------------------------------------------------------
	class CRange : public CRefCount
	{
		public:

			enum ERangeInclusion
			{
				EriIncluded,
				EriExcluded,
				EriSentinel
			};

		private:
			// range type
			IMDId *m_pmdid;

			// datum comparator
			const IComparator *m_pcomp;

			// left end point, NULL if infinite
			IDatum *m_pdatumLeft;

			// inclusion option for left end
			ERangeInclusion m_eriLeft;

			// right end point, NULL if infinite
			IDatum *m_pdatumRight;

			// inclusion option for right end
			ERangeInclusion m_eriRight;

			// hidden copy ctor
			CRange(const CRange&);

			// construct an equality predicate if possible
			CExpression *PexprEquality(IMemoryPool *pmp, const CColRef *pcr);

			// construct a scalar comparison expression from one of the ends
			CExpression *PexprScalarCompEnd
							(
							IMemoryPool *pmp,
							IDatum *pdatum,
							ERangeInclusion eri,
							IMDType::ECmpType ecmptIncl,
							IMDType::ECmpType ecmptExcl,
							const CColRef *pcr
							);

			// inverse of the inclusion option
			ERangeInclusion EriInverseInclusion
								(
								ERangeInclusion eri
								)
			{
				if (EriIncluded == eri)
				{
					return EriExcluded;
				}
				return EriIncluded;
			}

			// print a bound
			IOstream &OsPrintBound(IOstream &os, IDatum *pdatum, const CHAR *szInfinity) const;

		public:

			// ctor
			CRange
				(
				IMDId *pmdid,
				const IComparator *pcomp,
				IDatum *pdatumLeft,
				ERangeInclusion eriLeft,
				IDatum *pdatumRight,
				ERangeInclusion eriRight
				);

			// ctor
			CRange(const IComparator *pcomp, IMDType::ECmpType ecmpt, IDatum *pdatum);

			// dtor
			virtual
			~CRange();

			// range type
			IMDId *Pmdid() const
			{
				return m_pmdid;
			}

			// range beginning
			IDatum *PdatumLeft() const
			{
				return m_pdatumLeft;
			}

			// range end
			IDatum *PdatumRight() const
			{
				return m_pdatumRight;
			}

			// left end inclusion
			ERangeInclusion EriLeft() const
			{
				return m_eriLeft;
			}

			// right end inclusion
			ERangeInclusion EriRight() const
			{
				return m_eriRight;
			}

			// is this range disjoint from the given range and to its left
			BOOL FDisjointLeft(CRange *prange);

			// does this range contain the given range
			BOOL FContains(CRange *prange);

			// does this range overlap only the left end of the given range
			BOOL FOverlapsLeft(CRange *prange);

			// does this range overlap only the right end of the given range
			BOOL FOverlapsRight(CRange *prange);

			// does this range's upper bound equal the given range's lower bound
			BOOL FUpperBoundEqualsLowerBound(CRange *prange);

			// does this range start before the given range starts
			BOOL FStartsBefore(CRange *prange);

			// does this range start with or before the given range
			BOOL FStartsWithOrBefore(CRange *prange);

			// does this range end after the given range ends
			BOOL FEndsAfter(CRange *prange);

			// does this range end with or after the given range
			BOOL FEndsWithOrAfter(CRange *prange);
			
			// check if range represents a point
			BOOL FPoint() const;

			// intersection with another range
			CRange *PrngIntersect(IMemoryPool *pmp, CRange *prange);

			// difference between this range and a given range on the left side only
			CRange *PrngDifferenceLeft(IMemoryPool *pmp, CRange *prange);

			// difference between this range and a given range on the right side only
			CRange *PrngDifferenceRight(IMemoryPool *pmp, CRange *prange);

			// return the extension of this range with the given range. The given
			// range must start right after this range, otherwise NULL is returned
			CRange *PrngExtend(IMemoryPool *pmp, CRange *prange);

			// construct scalar expression
			CExpression *PexprScalar(IMemoryPool *pmp, const CColRef *pcr);

			// is this interval unbounded
			BOOL FUnbounded() const
			{
				return (NULL == m_pdatumLeft && NULL == m_pdatumRight);
			}

			// print
			IOstream &OsPrint(IOstream &os) const;

			// debug print
			void DbgPrint() const;

	}; // class CRange

 	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CRange &range)
	{
		return range.OsPrint(os);
	}
}

#endif // !GPOPT_CRange_H

// EOF
