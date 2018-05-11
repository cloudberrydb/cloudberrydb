//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalIntersect.h
//
//	@doc:
//		Logical Intersect operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalIntersect_H
#define GPOPT_CLogicalIntersect_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSetOp.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalIntersect
	//
	//	@doc:
	//		Intersect operators
	//
	//---------------------------------------------------------------------------
	class CLogicalIntersect : public CLogicalSetOp
	{

		private:

			// private copy ctor
			CLogicalIntersect(const CLogicalIntersect &);

		public:

			// ctor
			explicit
			CLogicalIntersect(IMemoryPool *mp);

			CLogicalIntersect
				(
				IMemoryPool *mp,
				CColRefArray *pdrgpcrOutput,
				CColRef2dArray *pdrgpdrgpcrInput
				);

			// dtor
			virtual
			~CLogicalIntersect();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalIntersect;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalIntersect";
			}

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintIntersectUnion(mp, exprhdl, true /*fIntersect*/);
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			CXformSet *PxfsCandidates(IMemoryPool *mp) const;

			//-------------------------------------------------------------------------------------
			// Derived Stats
			//-------------------------------------------------------------------------------------

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			// derive statistics
			virtual
			IStatistics *PstatsDerive
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				IStatisticsArray *stats_ctxt
				)
				const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalIntersect *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalIntersect == pop->Eopid());

				return reinterpret_cast<CLogicalIntersect*>(pop);
			}

	}; // class CLogicalIntersect

}


#endif // !GPOPT_CLogicalIntersect_H

// EOF
