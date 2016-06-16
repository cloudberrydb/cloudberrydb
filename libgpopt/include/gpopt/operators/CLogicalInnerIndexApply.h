//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CLogicalInnerIndexApply.h
//
//	@doc:
//		Inner Index Apply operator;
//		a variant of inner apply that captures the need to implement a
//		correlated-execution strategy on the physical side, where the inner
//		side is an index scan with parameters from outer side
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalInnerIndexApply_H
#define GPOPT_CLogicalInnerIndexApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalInnerIndexApply
	//
	//	@doc:
	//		Index Apply operator
	//
	//---------------------------------------------------------------------------
	class CLogicalInnerIndexApply : public CLogicalApply
	{

		private:

			// columns used from Apply's outer child used by index in Apply's inner child
			DrgPcr *m_pdrgpcrOuterRefs;

			// private copy ctor
			CLogicalInnerIndexApply(const CLogicalInnerIndexApply &);

		public:

			// ctor
			CLogicalInnerIndexApply(IMemoryPool *pmp,  DrgPcr *pdrgpcrOuterRefs);

			// ctor for patterns
			explicit
			CLogicalInnerIndexApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CLogicalInnerIndexApply();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalInnerIndexApply;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalInnerIndexApply";
			}

			// outer column references accessor
			DrgPcr *PdrgPcrOuterRefs() const
			{
				return m_pdrgpcrOuterRefs;
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
			{
				GPOS_ASSERT(3 == exprhdl.UlArity());

				return PcrsDeriveOutputCombineLogical(pmp, exprhdl);
			}

			// derive not nullable columns
			virtual
			CColRefSet *PcrsDeriveNotNull
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PcrsDeriveNotNullCombineLogical(pmp, exprhdl);
			}

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintFromPredicates(pmp, exprhdl);
			}

			// applicable transformations
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			//-------------------------------------------------------------------------------------
			// Derived Stats
			//-------------------------------------------------------------------------------------

			// derive statistics
			virtual
			IStatistics *PstatsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl, DrgPstat *pdrgpstatCtxt) const;

			// stat promise
			virtual
			EStatPromise Esp
				(
				CExpressionHandle & // exprhdl
				)
				const
			{
				return CLogical::EspMedium;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			// conversion function
			static
			CLogicalInnerIndexApply *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalInnerIndexApply == pop->Eopid());

				return dynamic_cast<CLogicalInnerIndexApply*>(pop);
			}

	}; // class CLogicalInnerIndexApply

}


#endif // !GPOPT_CLogicalInnerIndexApply_H

// EOF
