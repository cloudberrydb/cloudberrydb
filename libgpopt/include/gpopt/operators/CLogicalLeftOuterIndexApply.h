//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Left Outer Index Apply operator;
//	a variant of outer apply that captures the need to implement a
//	correlated-execution strategy on the physical side, where the inner
//	side is an index scan with parameters from outer side
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLeftOuterIndexApply_H
#define GPOPT_CLogicalLeftOuterIndexApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{

	class CLogicalLeftOuterIndexApply : public CLogicalApply
	{

		private:

			// columns used from Apply's outer child used by index in Apply's inner child
			DrgPcr *m_pdrgpcrOuterRefs;

			// private copy ctor
			CLogicalLeftOuterIndexApply(const CLogicalLeftOuterIndexApply &);

		public:

			// ctor
			CLogicalLeftOuterIndexApply(IMemoryPool *pmp,  DrgPcr *pdrgpcrOuterRefs);

			// ctor for patterns
			explicit
			CLogicalLeftOuterIndexApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CLogicalLeftOuterIndexApply();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalLeftOuterIndexApply;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalLeftOuterIndexApply";
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
			CLogicalLeftOuterIndexApply *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalLeftOuterIndexApply == pop->Eopid());

				return dynamic_cast<CLogicalLeftOuterIndexApply*>(pop);
			}

	}; // class CLogicalLeftOuterIndexApply

}


#endif // !GPOPT_CLogicalLeftOuterIndexApply_H

// EOF
