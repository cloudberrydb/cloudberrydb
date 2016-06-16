//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftAntiSemiApply.h
//
//	@doc:
//		Logical Left Anti Semi Apply operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLeftAntiSemiApply_H
#define GPOPT_CLogicalLeftAntiSemiApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalLeftAntiSemiApply
	//
	//	@doc:
	//		Logical Left Anti Semi Apply operator
	//
	//---------------------------------------------------------------------------
	class CLogicalLeftAntiSemiApply : public CLogicalApply
	{

		private:

			// private copy ctor
			CLogicalLeftAntiSemiApply(const CLogicalLeftAntiSemiApply &);

		public:

			// ctor
			explicit
			CLogicalLeftAntiSemiApply
				(
				IMemoryPool *pmp
				)
				:
				CLogicalApply(pmp)
			{}

			// ctor
			CLogicalLeftAntiSemiApply
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrInner,
				EOperatorId eopidOriginSubq
				)
				:
				CLogicalApply(pmp, pdrgpcrInner, eopidOriginSubq)
			{}

			// dtor
			virtual
			~CLogicalLeftAntiSemiApply()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalLeftAntiSemiApply;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalLeftAntiSemiApply";
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

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput
				(
				IMemoryPool *, // pmp
				CExpressionHandle &exprhdl
				)
			{
				GPOS_ASSERT(3 == exprhdl.UlArity());

				return PcrsDeriveOutputPassThru(exprhdl);
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
				return PcrsDeriveNotNullPassThruOuter(exprhdl);
			}

			// dervive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;
						
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
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// return true if operator is a left anti semi apply
			virtual
			BOOL FLeftAntiSemiApply() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			// conversion function
			static
			CLogicalLeftAntiSemiApply *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(CUtils::FLeftAntiSemiApply(pop));

				return dynamic_cast<CLogicalLeftAntiSemiApply*>(pop);
			}

	}; // class CLogicalLeftAntiSemiApply

}


#endif // !GPOPT_CLogicalLeftAntiSemiApply_H

// EOF
