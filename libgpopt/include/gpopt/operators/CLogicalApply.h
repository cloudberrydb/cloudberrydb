//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2011 EMC Corp.
//
//	@filename:
//		CLogicalApply.h
//
//	@doc:
//		Logical Apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalApply_H
#define GPOPT_CLogicalApply_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalApply
	//
	//	@doc:
	//		Logical Apply operator; parent of different Apply operators used
	//		in subquery transformations
	//
	//---------------------------------------------------------------------------
	class CLogicalApply : public CLogical
	{

		private:

			// private copy ctor
			CLogicalApply(const CLogicalApply &);

		protected:

			// columns used from Apply's inner child
			DrgPcr *m_pdrgpcrInner;

			// origin subquery id
			EOperatorId m_eopidOriginSubq;

			// ctor
			explicit
			CLogicalApply(IMemoryPool *pmp);

			// ctor
			CLogicalApply(IMemoryPool *pmp, DrgPcr *pdrgpcrInner, EOperatorId eopidOriginSubq);

			// dtor
			virtual
			~CLogicalApply();

		public:

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// inner column references accessor
			DrgPcr *PdrgPcrInner() const
			{
				return m_pdrgpcrInner;
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
				return PfpDeriveFromScalar(pmp, exprhdl, 2 /*ulScalarIndex*/);
			}

			//-------------------------------------------------------------------------------------
			// Derived Stats
			//-------------------------------------------------------------------------------------

			// derive statistics
			virtual
			IStatistics *PstatsDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				DrgPstat *// pdrgpstatCtxt
				)
				const
			{
				// we should use stats from the corresponding Join tree if decorrelation succeeds
				return PstatsDeriveDummy(pmp, exprhdl, CStatistics::DDefaultRelationRows);
			}

			// promise level for stat derivation
			virtual
			EStatPromise Esp
				(
				CExpressionHandle & // exprhdl
				)
				const
			{
				// whenever we can decorrelate an Apply tree, we should use the corresponding Join tree
				return EspLow;
			}

			//-------------------------------------------------------------------------------------
			// Required Relational Properties
			//-------------------------------------------------------------------------------------

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat(IMemoryPool *pmp, CExpressionHandle &exprhdl, CColRefSet *pcrsInput, ULONG ulChildIndex) const;

			// return true if operator is a correlated apply
			virtual
			BOOL FCorrelated() const
			{
				return false;
			}

			// return true if operator is a left semi apply
			virtual
			BOOL FLeftSemiApply() const
			{
				return false;
			}

			// return true if operator is a left anti semi apply
			virtual
			BOOL FLeftAntiSemiApply() const
			{
				return false;
			}

			// return true if operator can select a subset of input tuples based on some predicate
			virtual
			BOOL FSelectionOp() const
			{
				return true;
			}

			// origin subquery id
			EOperatorId EopidOriginSubq() const
			{
				return m_eopidOriginSubq;
			}

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// conversion function
			static
			CLogicalApply *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(CUtils::FApply(pop));

				return dynamic_cast<CLogicalApply*>(pop);
			}

	}; // class CLogicalApply

}


#endif // !GPOPT_CLogicalApply_H

// EOF
