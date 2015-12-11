//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEConsumer.h
//
//	@doc:
//		Logical CTE consumer operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalCTEConsumer_H
#define GPOPT_CLogicalCTEConsumer_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalCTEConsumer
	//
	//	@doc:
	//		CTE consumer operator
	//
	//---------------------------------------------------------------------------
	class CLogicalCTEConsumer : public CLogical
	{
		private:

			// cte identifier
			ULONG m_ulId;

			// mapped cte columns
			DrgPcr *m_pdrgpcr;

			// inlined expression
			CExpression *m_pexprInlined;

			// map of CTE producer's output column ids to consumer's output columns
			HMUlCr *m_phmulcr;

			// output columns
			CColRefSet *m_pcrsOutput;

			// create the inlined version of this consumer as well as the column mapping
			void CreateInlinedExpr(IMemoryPool *pmp);

			// private copy ctor
			CLogicalCTEConsumer(const CLogicalCTEConsumer &);

		public:

			// ctor
			explicit
			CLogicalCTEConsumer(IMemoryPool *pmp);

			// ctor
			CLogicalCTEConsumer(IMemoryPool *pmp, ULONG ulId, DrgPcr *pdrgpcr);

			// dtor
			virtual
			~CLogicalCTEConsumer();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalCTEConsumer;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CLogicalCTEConsumer";
			}

			// cte identifier
			ULONG UlCTEId() const
			{
				return m_ulId;
			}

			// cte columns
			DrgPcr *Pdrgpcr() const
			{
				return m_pdrgpcr;
			}

			// column mapping
			HMUlCr *Phmulcr() const
			{
				return m_phmulcr;
			}

			CExpression *PexprInlined() const
			{
				return m_pexprInlined;
			}

			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const;

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// dervive keys
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive join depth
			virtual
			ULONG UlJoinDepth(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive not nullable output columns
			virtual
			CColRefSet *PcrsDeriveNotNull(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// compute required stats columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *,// pmp
				CExpressionHandle &,// exprhdl
				CColRefSet *, //pcrsInput,
				ULONG // ulChildIndex
				)
				const
			{
				GPOS_ASSERT(!"CLogicalCTEConsumer has no children");
				return NULL;
			}

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
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
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalCTEConsumer *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalCTEConsumer == pop->Eopid());

				return dynamic_cast<CLogicalCTEConsumer*>(pop);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalCTEConsumer

}

#endif // !GPOPT_CLogicalCTEConsumer_H

// EOF
