//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalJoin.h
//
//	@doc:
//		Base class of all logical join operators
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalJoin_H
#define GPOS_CLogicalJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalJoin
	//
	//	@doc:
	//		join operator
	//
	//---------------------------------------------------------------------------
	class CLogicalJoin : public CLogical
	{
		private:

			// private copy ctor
			CLogicalJoin(const CLogicalJoin &);

		protected:

			// ctor
			explicit
			CLogicalJoin(IMemoryPool *pmp);
		
			// dtor
			virtual 
			~CLogicalJoin() 
			{}

		public:
		
			// match function
			virtual
			BOOL FMatch(COperator *pop) const;


			// sensitivity to order of inputs
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

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
			{
				return PcrsDeriveOutputCombineLogical(pmp, exprhdl);
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

			
			// derive keys
			CKeyCollection *PkcDeriveKeys
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PkcCombineKeys(pmp, exprhdl);
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
				return PfpDeriveFromScalar(pmp, exprhdl, exprhdl.UlArity() - 1);
			}

			//-------------------------------------------------------------------------------------
			// Derived Stats
			//-------------------------------------------------------------------------------------

			// promise level for stat derivation
			virtual
			EStatPromise Esp
				(
				CExpressionHandle &exprhdl
				)
				const
			{
				// no stat derivation on Join trees with subqueries
				if (exprhdl.Pdpscalar(exprhdl.UlArity() - 1)->FHasSubquery())
				{
					 return EspLow;
				}

				if (NULL != exprhdl.Pgexpr() &&
					exprhdl.Pgexpr()->ExfidOrigin() == CXform::ExfExpandNAryJoin)
				{
					return EspMedium;
				}

				return EspHigh;
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
				const ULONG ulArity = exprhdl.UlArity();

				return PcrsReqdChildStats(pmp, exprhdl, pcrsInput, exprhdl.Pdpscalar(ulArity - 1)->PcrsUsed(), ulChildIndex);
			}

			// return true if operator can select a subset of input tuples based on some predicate
			virtual
			BOOL FSelectionOp() const
			{
				return true;
			}

	}; // class CLogicalJoin

}


#endif // !GPOS_CLogicalJoin_H

// EOF
