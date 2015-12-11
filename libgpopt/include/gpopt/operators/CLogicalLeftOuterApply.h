//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CLogicalLeftOuterApply.h
//
//	@doc:
//		Logical Left Outer Apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLeftOuterApply_H
#define GPOPT_CLogicalLeftOuterApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalLeftOuterApply
	//
	//	@doc:
	//		Logical left outer Apply operator used in subquery transformations
	//
	//---------------------------------------------------------------------------
	class CLogicalLeftOuterApply : public CLogicalApply
	{

		private:

			// private copy ctor
			CLogicalLeftOuterApply(const CLogicalLeftOuterApply &);

		public:

			// ctor for patterns
			explicit
			CLogicalLeftOuterApply(IMemoryPool *pmp);

			// ctor
			CLogicalLeftOuterApply(IMemoryPool *pmp, DrgPcr *pdrgpcrInner, EOperatorId eopidOriginSubq);

			// dtor
			virtual
			~CLogicalLeftOuterApply();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalLeftOuterApply;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalLeftOuterApply";
			}

			// return true if we can pull projections up past this operator from its given child
			virtual
			BOOL FCanPullProjectionsUp
				(
				ULONG ulChildIndex
				) const
			{
				return (0 == ulChildIndex);
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

			// derive not nullable output columns
			virtual
			CColRefSet *PcrsDeriveNotNull
				(
				IMemoryPool *,// pmp
				CExpressionHandle &exprhdl
				)
				const
			{
				// left outer apply passes through not null columns from outer child only
				return PcrsDeriveNotNullPassThruOuter(exprhdl);
			}

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, //pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
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
			CLogicalLeftOuterApply *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalLeftOuterApply == pop->Eopid());

				return dynamic_cast<CLogicalLeftOuterApply*>(pop);
			}

	}; // class CLogicalLeftOuterApply

}


#endif // !GPOPT_CLogicalLeftOuterApply_H

// EOF
