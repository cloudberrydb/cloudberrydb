//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalGbAgg.h
//
//	@doc:
//		Group Aggregate operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalGbAgg_H
#define GPOS_CLogicalGbAgg_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{
	// fwd declaration
	class CColRefSet;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalGbAgg
	//
	//	@doc:
	//		aggregate operator
	//
	//---------------------------------------------------------------------------
	class CLogicalGbAgg : public CLogicalUnary
	{
		private:

			// private copy ctor
			CLogicalGbAgg(const CLogicalGbAgg &);
			
			// array of grouping columns
			DrgPcr *m_pdrgpcr;

			// minimal grouping columns based on FD's
			DrgPcr *m_pdrgpcrMinimal;

			// local / intermediate / global aggregate
			COperator::EGbAggType m_egbaggtype;

		protected:

			// does local / intermediate / global aggregate generate duplicate values for the same group
			BOOL m_fGeneratesDuplicates;

			// array of columns used in distinct qualified aggregates (DQA)
			// used only in the case of intermediate aggregates
			DrgPcr *m_pdrgpcrArgDQA;

			// compute required stats columns for a GbAgg
			CColRefSet *PcrsStatGbAgg
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsInput,
				ULONG ulChildIndex,
				DrgPcr *pdrgpcrGrp
				)
				const;

		public:

			// ctor
			explicit
			CLogicalGbAgg(IMemoryPool *pmp);

			// ctor
			CLogicalGbAgg
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				COperator::EGbAggType egbaggtype
				);

			// ctor
			CLogicalGbAgg
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				COperator::EGbAggType egbaggtype,
				BOOL fGeneratesDuplicates,
				DrgPcr *pdrgpcrArgDQA
				);

			// ctor
			CLogicalGbAgg
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				DrgPcr *pdrgpcrMinimal,
				COperator::EGbAggType egbaggtype
				);

			// ctor
			CLogicalGbAgg
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				DrgPcr *pdrgpcrMinimal,
				COperator::EGbAggType egbaggtype,
				BOOL fGeneratesDuplicates,
				DrgPcr *pdrgpcrArgDQA
				);

			// dtor
			virtual
			~CLogicalGbAgg();

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalGbAgg;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalGbAgg";
			}

			// does this aggregate generate duplicate values for the same group
			virtual
			BOOL FGeneratesDuplicates() const
			{
				return m_fGeneratesDuplicates;
			}

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// hash function
			virtual
			ULONG UlHash() const;

			// grouping columns accessor
			DrgPcr *Pdrgpcr() const
			{
				return m_pdrgpcr;
			}

			// array of columns used in distinct qualified aggregates (DQA)
			DrgPcr *PdrgpcrArgDQA() const
			{
				return m_pdrgpcrArgDQA;
			}

			// aggregate type
			COperator::EGbAggType Egbaggtype() const
			{
				return m_egbaggtype;
			}

			// is a global aggregate?
			BOOL FGlobal() const
			{
				return (COperator::EgbaggtypeGlobal == m_egbaggtype);
			}

			// minimal grouping columns accessor
			DrgPcr *PdrgpcrMinimal() const
			{
				return m_pdrgpcrMinimal;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *,CExpressionHandle &);
			
			// derive outer references
			virtual
			CColRefSet *PcrsDeriveOuter(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// derive not null columns
			virtual
			CColRefSet *PcrsDeriveNotNull(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// compute required stats columns of the n-th child
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
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						DrgPstat *pdrgpstatCtxt
						)
						const;

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalGbAgg *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalGbAgg == pop->Eopid() ||
							EopLogicalGbAggDeduplicate == pop->Eopid());

				return dynamic_cast<CLogicalGbAgg*>(pop);
			}

		
			// debug print
			virtual 
			IOstream &OsPrint(IOstream &os) const;
		
			// derive statistics
			static
			IStatistics *PstatsDerive
				(
				IMemoryPool *pmp,
				IStatistics *pstatsChild,
				DrgPcr *pdrgpcrGroupingCols,
				DrgPul *pdrgpulComputedCols,
				CBitSet *pbsKeys
				);

			// print group by aggregate type
			static
			IOstream &OsPrintGbAggType(IOstream &os, COperator::EGbAggType egbaggtype);

	}; // class CLogicalGbAgg

}


#endif // !GPOS_CLogicalGbAgg_H

// EOF
