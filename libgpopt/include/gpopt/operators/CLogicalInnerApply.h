//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2011 EMC Corp.
//
//	@filename:
//		CLogicalInnerApply.h
//
//	@doc:
//		Logical Inner Apply operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalInnerApply_H
#define GPOPT_CLogicalInnerApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalInnerApply
	//
	//	@doc:
	//		Logical Apply operator used in scalar subquery transformations
	//
	//---------------------------------------------------------------------------
	class CLogicalInnerApply : public CLogicalApply
	{

		private:
			// private copy ctor
			CLogicalInnerApply(const CLogicalInnerApply &);

		public:

			// ctor for patterns
			explicit
			CLogicalInnerApply(IMemoryPool *pmp);

			// ctor
			CLogicalInnerApply(IMemoryPool *pmp, DrgPcr *pdrgpcrInner, EOperatorId eopidOriginSubq);

			// dtor
			virtual
			~CLogicalInnerApply();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalInnerApply;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalInnerApply";
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

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

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalInnerApply *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalInnerApply == pop->Eopid());

				return dynamic_cast<CLogicalInnerApply*>(pop);
			}

	}; // class CLogicalInnerApply

}


#endif // !GPOPT_CLogicalInnerApply_H

// EOF
