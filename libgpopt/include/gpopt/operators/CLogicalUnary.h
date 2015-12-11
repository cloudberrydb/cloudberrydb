//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalUnary.h
//
//	@doc:
//		Base class of logical unary operators
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalUnary_H
#define GPOS_CLogicalUnary_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"

#include "naucrates/base/IDatum.h"

namespace gpopt
{
	using namespace gpnaucrates;

	// fwd declaration
	class CColRefSet;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalUnary
	//
	//	@doc:
	//		Base class of logical unary operators
	//
	//---------------------------------------------------------------------------
	class CLogicalUnary : public CLogical
	{
		private:

			// private copy ctor
			CLogicalUnary(const CLogicalUnary &);

		protected:

			// derive statistics for projection operators
			IStatistics *PstatsDeriveProject
							(
							IMemoryPool *pmp,
							CExpressionHandle &exprhdl,
							HMUlDatum *phmuldatum = NULL
							)
							const;

		public:

			// ctor
			explicit
			CLogicalUnary
				(
				IMemoryPool *pmp
				)
				:
				CLogical(pmp)
			{}

			// dtor
			virtual
			~CLogicalUnary()
			{}

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns
						(
						IMemoryPool *, //pmp,
						HMUlCr *, //phmulcr,
						BOOL //fMustExist
						)
			{
				return PopCopyDefault();
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive not nullable output columns
			virtual
			CColRefSet *PcrsDeriveNotNull
				(
				IMemoryPool *,// pmp
				CExpressionHandle &exprhdl
				)
				const
			{
				// TODO,  03/18/2012, derive nullability of columns computed by scalar child
				return PcrsDeriveNotNullPassThruOuter(exprhdl);
			}

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpartinfoDeriveCombine(pmp, exprhdl);
			}
			
			// derive function properties
			virtual
			CFunctionProp *PfpDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PfpDeriveFromScalar(pmp, exprhdl, 1 /*ulScalarIndex*/);
			}

			//-------------------------------------------------------------------------------------
			// Derived Stats
			//-------------------------------------------------------------------------------------

			// promise level for stat derivation
			virtual
			EStatPromise Esp(CExpressionHandle &exprhdl) const;

			//-------------------------------------------------------------------------------------
			// Required Relational Properties
			//-------------------------------------------------------------------------------------

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsInput,
				ULONG ulChildIndex
				)
				const
			{
				return PcrsReqdChildStats(pmp, exprhdl, pcrsInput, exprhdl.Pdpscalar(1)->PcrsUsed(), ulChildIndex);
			}

	}; // class CLogicalUnary

}

#endif // !GPOS_CLogicalUnary_H

// EOF
