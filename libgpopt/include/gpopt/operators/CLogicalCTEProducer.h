//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEProducer.h
//
//	@doc:
//		Logical CTE producer operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalCTEProducer_H
#define GPOPT_CLogicalCTEProducer_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalCTEProducer
	//
	//	@doc:
	//		CTE producer operator
	//
	//---------------------------------------------------------------------------
	class CLogicalCTEProducer : public CLogical
	{
		private:

			// cte identifier
			ULONG m_ulId;

			// cte columns
			DrgPcr *m_pdrgpcr;

			// output columns, same as cte columns but in CColRefSet
			CColRefSet *m_pcrsOutput;

			// private copy ctor
			CLogicalCTEProducer(const CLogicalCTEProducer &);

		public:

			// ctor
			explicit
			CLogicalCTEProducer(IMemoryPool *pmp);

			// ctor
			CLogicalCTEProducer(IMemoryPool *pmp, ULONG ulId, DrgPcr *pdrgpcr);

			// dtor
			virtual
			~CLogicalCTEProducer();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalCTEProducer;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CLogicalCTEProducer";
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

			// cte columns in CColRefSet
			CColRefSet *pcrsOutput() const
			{
				return m_pcrsOutput;
			}

			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return false;
			}

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

			// derive not nullable output columns
			virtual
			CColRefSet *PcrsDeriveNotNull(IMemoryPool *pmp,	CExpressionHandle &exprhdl)	const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintRestrict(pmp, exprhdl, m_pcrsOutput);
			}

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *, // pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpartinfoPassThruOuter(exprhdl);
			}

			// compute required stats columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *,// pmp
				CExpressionHandle &,// exprhdl
				CColRefSet *pcrsInput,
				ULONG // ulChildIndex
				)
				const
			{
				return PcrsStatsPassThru(pcrsInput);
			}

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *, //pmp,
						CExpressionHandle &exprhdl,
						DrgPstat * //pdrgpstatCtxt
						)
						const
			{
				return PstatsPassThruOuter(exprhdl);
			}

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalCTEProducer *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalCTEProducer == pop->Eopid());

				return dynamic_cast<CLogicalCTEProducer*>(pop);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalCTEProducer

}

#endif // !GPOPT_CLogicalCTEProducer_H

// EOF
