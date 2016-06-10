//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalLeftAntiSemiCorrelatedApplyNotIn.h
//
//	@doc:
//		Logical Left Anti Semi Correlated Apply operator;
//		a variant of left anti semi apply (for ALL/NOT IN subqueries)
//		to capture the need to implement a correlated-execution strategy
//		on the physical side
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLeftAntiSemiCorrelatedApplyNotIn_H
#define GPOPT_CLogicalLeftAntiSemiCorrelatedApplyNotIn_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalLeftAntiSemiApplyNotIn.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalLeftAntiSemiCorrelatedApplyNotIn
	//
	//	@doc:
	//		Logical Apply operator used in correlated execution of NOT IN/ALL subqueries
	//
	//---------------------------------------------------------------------------
	class CLogicalLeftAntiSemiCorrelatedApplyNotIn : public CLogicalLeftAntiSemiApplyNotIn
	{

		private:

			// private copy ctor
			CLogicalLeftAntiSemiCorrelatedApplyNotIn(const CLogicalLeftAntiSemiCorrelatedApplyNotIn &);

		public:

			// ctor
			explicit
			CLogicalLeftAntiSemiCorrelatedApplyNotIn
				(
				IMemoryPool *pmp
				)
				:
				CLogicalLeftAntiSemiApplyNotIn(pmp)
			{}

			// ctor
			CLogicalLeftAntiSemiCorrelatedApplyNotIn
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrInner,
				EOperatorId eopidOriginSubq
				)
				:
				CLogicalLeftAntiSemiApplyNotIn(pmp, pdrgpcrInner, eopidOriginSubq)
			{}

			// dtor
			virtual
			~CLogicalLeftAntiSemiCorrelatedApplyNotIn()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalLeftAntiSemiCorrelatedApplyNotIn;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalLeftAntiSemiCorrelatedApplyNotIn";
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// return true if operator is a correlated apply
			virtual
			BOOL FCorrelated() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			// conversion function
			static
			CLogicalLeftAntiSemiCorrelatedApplyNotIn *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalLeftAntiSemiCorrelatedApplyNotIn == pop->Eopid());

				return dynamic_cast<CLogicalLeftAntiSemiCorrelatedApplyNotIn*>(pop);
			}

	}; // class CLogicalLeftAntiSemiCorrelatedApplyNotIn

}


#endif // !GPOPT_CLogicalLeftAntiSemiCorrelatedApplyNotIn_H

// EOF
