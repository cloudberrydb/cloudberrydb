//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	Base Index Apply operator for Inner and Outer Join;
//	a variant of inner/outer apply that captures the need to implement a
//	correlated-execution strategy on the physical side, where the inner
//	side is an index scan with parameters from outer side
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalIndexApply_H
#define GPOPT_CLogicalIndexApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{

	class CLogicalIndexApply : public CLogicalApply
	{
		private:

			// private copy ctor
			CLogicalIndexApply(const CLogicalIndexApply &);

		protected:

			// columns used from Apply's outer child used by index in Apply's inner child
			DrgPcr *m_pdrgpcrOuterRefs;

			// is this an outer join?
			BOOL m_fOuterJoin;

		public:

			// ctor
			CLogicalIndexApply(IMemoryPool *pmp,  DrgPcr *pdrgpcrOuterRefs, BOOL fOuterJoin);

			// ctor for patterns
			explicit
			CLogicalIndexApply(IMemoryPool *pmp);

			// dtor
			virtual
			~CLogicalIndexApply();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalIndexApply;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalIndexApply";
			}

			// outer column references accessor
			DrgPcr *PdrgPcrOuterRefs() const
			{
				return m_pdrgpcrOuterRefs;
			}

			// outer column references accessor
			BOOL FouterJoin() const
			{
				return m_fOuterJoin;
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
			CLogicalIndexApply *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalIndexApply == pop->Eopid());

				return dynamic_cast<CLogicalIndexApply*>(pop);
			}

	}; // class CLogicalIndexApply

}


#endif // !GPOPT_CLogicalIndexApply_H

// EOF
