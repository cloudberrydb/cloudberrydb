//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintInterval.h
//
//	@doc:
//		Representation of an interval constraint. An interval contains a number
//		of ranges + "is null" and "is not null" flags. The interval can be interpreted
//		as the ORing of the ranges and the flags that are set
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraintInterval_H
#define GPOPT_CConstraintInterval_H

#include "gpos/base.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/error/CAutoTrace.h"
#include "naucrates/traceflags/traceflags.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CRange.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarArrayCmp.h"

namespace gpopt
{

	// range array
	typedef CDynamicPtrArray<CRange, CleanupRelease> DrgPrng;

	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CConstraintInterval
	//
	//	@doc:
	//		Representation of an interval constraint
	//
	//		If x has a CConstraintInterval C on it, this means that x is in the
	//		ranges contained in C.
	//
	//---------------------------------------------------------------------------
	class CConstraintInterval : public CConstraint
	{
		private:

			// column referenced in this constraint
			const CColRef *m_pcr;

			// array of ranges
			DrgPrng *m_pdrgprng;

			// does the interval include the null value
			BOOL m_fIncludesNull;

			// hidden copy ctor
			CConstraintInterval(const CConstraintInterval&);

			// adds ranges from a source array to a destination array, starting
			// at the range with the given index
			void AddRemainingRanges
					(
					IMemoryPool *pmp,
					DrgPrng *pdrgprngSrc,
					ULONG ulStart,
					DrgPrng *pdrgprngDest
					);

			// append the given range to the array or extend the last element
			void AppendOrExtend
					(
					IMemoryPool *pmp,
					DrgPrng *pdrgprng,
					CRange *prange
					);

			// difference between two ranges on the left side only -
			// any difference on the right side is reported as residual range
			CRange *PrangeDiffWithRightResidual
					(
					IMemoryPool *pmp,
					CRange *prangeFirst,
					CRange *prangeSecond,
					CRange **pprangeResidual,
					DrgPrng *pdrgprngResidual
					);

			// type of this interval
			IMDId *PmdidType();

			// construct scalar expression
			virtual
			CExpression *PexprConstructScalar(IMemoryPool *pmp) const;

			virtual
			CExpression *PexprConstructArrayScalar(IMemoryPool *pmp) const;

			// create interval from scalar comparison expression
			static
			CConstraintInterval *PciIntervalFromScalarCmp
									(
									IMemoryPool *pmp,
									CExpression *pexpr,
									CColRef *pcr
									);

			static
			CConstraintInterval *PciIntervalFromScalarIDF
									(
									IMemoryPool *pmp,
									CExpression *pexpr,
									CColRef *pcr
									);

			// create interval from scalar bool operator
			static
			CConstraintInterval *PciIntervalFromScalarBoolOp
									(
									IMemoryPool *pmp,
									CExpression *pexpr,
									CColRef *pcr
									);

			// create interval from scalar bool AND
			static
			CConstraintInterval *PciIntervalFromScalarBoolAnd
									(
									IMemoryPool *pmp,
									CExpression *pexpr,
									CColRef *pcr
									);

			// create interval from scalar bool OR
			static
			CConstraintInterval *PciIntervalFromScalarBoolOr
									(
									IMemoryPool *pmp,
									CExpression *pexpr,
									CColRef *pcr
									);

			// create interval from scalar null test
			static
			CConstraintInterval *PciIntervalFromScalarNullTest
									(
									IMemoryPool *pmp,
									CExpression *pexpr,
									CColRef *pcr
									);

			// creates a range like [x,x] where x is a constant
			static
			DrgPrng *PciRangeFromColConstCmp(IMemoryPool *pmp,
											 IMDType::ECmpType ecmpt,
											 const CScalarConst *popScConst);

			// create an array IN or NOT IN expression
			CExpression *
			PexprConstructArrayScalar(IMemoryPool *pmp, bool isIn) const;
		public:

			// ctor
			CConstraintInterval(IMemoryPool *pmp, const CColRef *pcr, DrgPrng *pdrgprng, BOOL fIsNull);

			// dtor
			virtual
			~CConstraintInterval();

			// constraint type accessor
			virtual
			EConstraintType Ect() const
			{
				return CConstraint::EctInterval;
			}

			// column referenced in constraint
			const CColRef *Pcr() const
			{
				return m_pcr;
			}

			// all ranges in interval
			DrgPrng *Pdrgprng() const
			{
				return m_pdrgprng;
			}

			// does the interval include the null value
			BOOL FIncludesNull() const
			{
				return m_fIncludesNull;
			}

			// is this constraint a contradiction
			virtual
			BOOL FContradiction() const;

			// is this interval unbounded
			virtual
			BOOL FUnbounded() const;

			// check if there is a constraint on the given column
			virtual
			BOOL FConstraint
					(
					const CColRef *pcr
					)
					const
			{
				return m_pcr == pcr;
			}

			// return a copy of the constraint with remapped columns
			virtual
			CConstraint *PcnstrCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			// interval intersection
			CConstraintInterval *PciIntersect(IMemoryPool *pmp, CConstraintInterval *pci);

			// interval union
			CConstraintInterval *PciUnion(IMemoryPool *pmp, CConstraintInterval *pci);

			// interval difference
			CConstraintInterval *PciDifference(IMemoryPool *pmp, CConstraintInterval *pci);

			// interval complement
			CConstraintInterval *PciComplement(IMemoryPool *pmp);

			// does the current interval contain the given interval?
			BOOL FContainsInterval(IMemoryPool *pmp, CConstraintInterval *pci);

			// scalar expression
			virtual
			CExpression *PexprScalar(IMemoryPool *pmp);

			// scalar expression  which will be a disjunction
			CExpression *PexprConstructDisjunctionScalar(IMemoryPool *pmp) const;

			// return constraint on a given column
			virtual
			CConstraint *Pcnstr(IMemoryPool *pmp, const CColRef *pcr);

			// return constraint on a given column set
			virtual
			CConstraint *Pcnstr(IMemoryPool *pmp, CColRefSet *pcrs);

			// return a clone of the constraint for a different column
			virtual
			CConstraint *PcnstrRemapForColumn(IMemoryPool *pmp, CColRef *pcr) const;

			// converts to an array in expression
			bool FConvertsToNotIn() const;

			// converts to an array not in expression
			bool FConvertsToIn() const;

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// create unbounded interval
			static
			CConstraintInterval *PciUnbounded(IMemoryPool *pmp, const CColRef *pcr, BOOL fIncludesNull);

			// create an unbounded interval on any column from the given set
			static
			CConstraintInterval *PciUnbounded(IMemoryPool *pmp,	const CColRefSet *pcrs,	BOOL fIncludesNull);

			// helper for create interval from comparison between a column and a constant
			static
			CConstraintInterval *PciIntervalFromColConstCmp
				(
				IMemoryPool *pmp,
				CColRef *pcr,
				IMDType::ECmpType ecmpt,
				CScalarConst *popScConst
				);

			// create interval from scalar expression
			static
			CConstraintInterval *PciIntervalFromScalarExpr
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				CColRef *pcr
				);

			// create interval from any general constraint that references
			// only one column
			static
			CConstraintInterval *PciIntervalFromConstraint
				(
				IMemoryPool *pmp,
				CConstraint *pcnstr,
				CColRef *pcr = NULL
				);

			// generate a ConstraintInterval from the given expression
			static
			CConstraintInterval *PcnstrIntervalFromScalarArrayCmp
				(
				IMemoryPool *pmp,
				CExpression *pexpr,
				CColRef *pcr
				);


#ifdef GPOS_DEBUG
			void DbgPrint() const;
#endif  // GPOS_DEBUG
	}; // class CConstraintInterval

	// shorthand for printing, reference
	inline
	IOstream &operator << (IOstream &os, const CConstraintInterval &interval)
	{
		return interval.OsPrint(os);
	}

	// shorthand for printing, pointer
	inline
	IOstream &operator << (IOstream &os, const CConstraintInterval *interval)
	{
		return interval->OsPrint(os);
	}

}

#endif // !GPOPT_CConstraintInterval_H

// EOF
