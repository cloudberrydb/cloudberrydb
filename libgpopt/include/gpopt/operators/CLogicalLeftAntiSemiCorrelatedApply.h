//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalLeftAntiSemiCorrelatedApply.h
//
//	@doc:
//		Logical Left Anti Semi Correlated Apply operator;
//		a variant of left anti semi apply (for NOT EXISTS subqueries)
//		to capture the need to implement a correlated-execution strategy
//		on the physical side
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLeftAntiSemiCorrelatedApply_H
#define GPOPT_CLogicalLeftAntiSemiCorrelatedApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalLeftAntiSemiApply.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalLeftAntiSemiCorrelatedApply
	//
	//	@doc:
	//		Logical Apply operator used in correlated execution of NOT EXISTS subqueries
	//
	//---------------------------------------------------------------------------
	class CLogicalLeftAntiSemiCorrelatedApply : public CLogicalLeftAntiSemiApply
	{

		private:

			// private copy ctor
			CLogicalLeftAntiSemiCorrelatedApply(const CLogicalLeftAntiSemiCorrelatedApply &);

		public:

			// ctor
			explicit
			CLogicalLeftAntiSemiCorrelatedApply
				(
				IMemoryPool *pmp
				)
				:
				CLogicalLeftAntiSemiApply(pmp)
			{}

			// ctor
			CLogicalLeftAntiSemiCorrelatedApply
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrInner,
				EOperatorId eopidOriginSubq
				)
				:
				CLogicalLeftAntiSemiApply(pmp, pdrgpcrInner, eopidOriginSubq)
			{}

			// dtor
			virtual
			~CLogicalLeftAntiSemiCorrelatedApply()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalLeftAntiSemiCorrelatedApply;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalLeftAntiSemiCorrelatedApply";
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
			CLogicalLeftAntiSemiCorrelatedApply *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalLeftAntiSemiCorrelatedApply == pop->Eopid());

				return dynamic_cast<CLogicalLeftAntiSemiCorrelatedApply*>(pop);
			}

	}; // class CLogicalLeftAntiSemiCorrelatedApply

}


#endif // !GPOPT_CLogicalLeftAntiSemiCorrelatedApply_H

// EOF
