//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalMaxOneRow.h
//
//	@doc:
//		MaxOneRow operator,
//		an operator that can pass at most one row from its input
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalMaxOneRow_H
#define GPOPT_CLogicalMaxOneRow_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalMaxOneRow
	//
	//	@doc:
	//		MaxOneRow operator
	//
	//---------------------------------------------------------------------------
	class CLogicalMaxOneRow : public CLogical
	{

		private:

			// private copy ctor
			CLogicalMaxOneRow(const CLogicalMaxOneRow &);

		public:

			// ctors
			explicit
			CLogicalMaxOneRow
				(
				IMemoryPool *pmp
				)
				:
				CLogical(pmp)
			{}


			// dtor
			virtual
			~CLogicalMaxOneRow() {}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalMaxOneRow;
			}

			// name of operator
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalMaxOneRow";
			}

			// match function;
			virtual
			BOOL FMatch
				(
				COperator *pop
				)
				const
			{
				return (Eopid() == pop->Eopid());
			}

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return false;
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

			// return true if we can pull projections up past this operator from its given child
			virtual
			BOOL FCanPullProjectionsUp
				(
				ULONG //ulChildIndex
				) const
			{
				return false;
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
				return PcrsDeriveOutputPassThru(exprhdl);
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

			// dervive keys
			virtual
			CKeyCollection *PkcDeriveKeys
				(
				IMemoryPool *, // pmp
				CExpressionHandle &exprhdl
				)
				const
			{
				return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
			}

			// derive max card
			virtual
			CMaxCard Maxcard
				(
				IMemoryPool *,  // pmp,
				CExpressionHandle & // exprhdl
				)
				const
			{
				return CMaxCard(1 /*ull*/);
			}

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
				const;

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates
				(
				IMemoryPool *pmp
				)
				const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalMaxOneRow *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalMaxOneRow == pop->Eopid());

				return reinterpret_cast<CLogicalMaxOneRow*>(pop);
			}

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						DrgPstat * // pdrgpstatCtxt
						)
						const;


	}; // class CLogicalMaxOneRow

}

#endif // !GPOPT_CLogicalMaxOneRow_H

// EOF
