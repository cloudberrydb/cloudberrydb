//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalIntersectAll.h
//
//	@doc:
//		Logical Intersect all operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalIntersectAll_H
#define GPOPT_CLogicalIntersectAll_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSetOp.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalIntersectAll
	//
	//	@doc:
	//		Intersect all operators
	//
	//---------------------------------------------------------------------------
	class CLogicalIntersectAll : public CLogicalSetOp
	{

		private:

			// private copy ctor
			CLogicalIntersectAll(const CLogicalIntersectAll &);

		public:

			// ctor
			explicit
			CLogicalIntersectAll(IMemoryPool *pmp);

			CLogicalIntersectAll
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrOutput,
				DrgDrgPcr *pdrgpdrgpcrInput
				);

			// dtor
			virtual
			~CLogicalIntersectAll();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalIntersectAll;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalIntersectAll";
			}

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintIntersectUnion(pmp, exprhdl, true /*fIntersect*/);
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

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
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				DrgPstat *pdrgpstatCtxt
				)
				const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			
			// conversion function
			static
			CLogicalIntersectAll *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalIntersectAll == pop->Eopid());

				return reinterpret_cast<CLogicalIntersectAll*>(pop);
			}

			// derive statistics
			static
			IStatistics *PstatsDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				DrgDrgPcr *pdrgpdrgpcrInput,
				DrgPcrs *pdrgpcrsOutput
				);

	}; // class CLogicalIntersectAll

}

#endif // !GPOPT_CLogicalIntersectAll_H

// EOF
