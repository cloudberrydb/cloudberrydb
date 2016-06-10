//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CPhysicalHashAggDeduplicate.h
//
//	@doc:
//		Hash Aggregate operator for deduplicating join outputs
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalHashAggDeduplicate_H
#define GPOS_CPhysicalHashAggDeduplicate_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalHashAgg.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalHashAggDeduplicate
	//
	//	@doc:
	//		Hash-based aggregate operator for deduplicating join outputs
	//
	//---------------------------------------------------------------------------
	class CPhysicalHashAggDeduplicate : public CPhysicalHashAgg
	{
		private:

			// array of keys from the join's child
			DrgPcr *m_pdrgpcrKeys;

			// private copy ctor
			CPhysicalHashAggDeduplicate(const CPhysicalHashAggDeduplicate &);

		public:

			// ctor
			CPhysicalHashAggDeduplicate
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				DrgPcr *pdrgpcrMinimal,
				COperator::EGbAggType egbaggtype,
				DrgPcr *pdrgpcrKeys,
				BOOL fGeneratesDuplicates,
				BOOL fMultiStage
				);

			// dtor
			virtual
			~CPhysicalHashAggDeduplicate();


			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalHashAggDeduplicate;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalHashAggDeduplicate";
			}

			// array of keys from the join's child
			DrgPcr *PdrgpcrKeys() const
			{
				return m_pdrgpcrKeys;
			}

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

			// compute required output columns of the n-th child
			virtual
			CColRefSet *PcrsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsRequired,
				ULONG ulChildIndex,
				DrgPdp *, //pdrgpdpCtxt,
				ULONG //ulOptReq
				)
			{
				return PcrsRequiredAgg(pmp, exprhdl, pcrsRequired, ulChildIndex, m_pdrgpcrKeys);
			}

			// compute required distribution of the n-th child
			virtual
			CDistributionSpec *PdsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsRequired,
				ULONG ulChildIndex,
				DrgPdp *, //pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const
			{
				return PdsRequiredAgg(pmp, exprhdl, pdsRequired, ulChildIndex, ulOptReq, m_pdrgpcrKeys, m_pdrgpcrKeys);
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// debug print
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// conversion function
			static
			CPhysicalHashAggDeduplicate *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalHashAggDeduplicate == pop->Eopid());

				return reinterpret_cast<CPhysicalHashAggDeduplicate*>(pop);
			}

	}; // class CPhysicalHashAggDeduplicate

}


#endif // !GPOS_CPhysicalHashAggDeduplicate_H

// EOF
