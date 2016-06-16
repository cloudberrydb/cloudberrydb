//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalAgg.h
//
//	@doc:
//		Basic physical aggregate operator
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalAgg_H
#define GPOS_CPhysicalAgg_H

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPhysical.h"


namespace gpopt
{
	// fwd declaration
	class CDistributionSpec;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalAgg
	//
	//	@doc:
	//		Aggregate operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalAgg : public CPhysical
	{
		private:

			// private copy ctor
			CPhysicalAgg(const CPhysicalAgg &);
			
			// array of grouping columns
			DrgPcr *m_pdrgpcr;

			// aggregate type (local / intermediate / global)
			COperator::EGbAggType m_egbaggtype;

			// compute required distribution of the n-th child of an intermediate aggregate
			CDistributionSpec *PdsRequiredIntermediateAgg(IMemoryPool *pmp, ULONG  ulOptReq) const;

			// compute required distribution of the n-th child of a global aggregate
			CDistributionSpec *PdsRequiredGlobalAgg
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsInput,
				ULONG ulChildIndex,
				DrgPcr *pdrgpcrGrp,
				DrgPcr *pdrgpcrGrpMinimal,
				ULONG  ulOptReq
				)
				const;

			// compute a maximal hashed distribution using the given columns,
			// if no such distribution can be created, return a Singleton distribution
			static
			CDistributionSpec *PdsMaximalHashed(IMemoryPool *pmp, DrgPcr *pdrgpcr);

		protected:

			// array of minimal grouping columns based on FDs
			DrgPcr *m_pdrgpcrMinimal;

			// could the local / intermediate / global aggregate generate
			// duplicate values for the same group across segments
			BOOL m_fGeneratesDuplicates;

			// array of columns used in distinct qualified aggregates (DQA)
			// used only in the case of intermediate aggregates
			DrgPcr *m_pdrgpcrArgDQA;

			// is agg part of multi-stage aggregation
			BOOL m_fMultiStage;

			// compute required columns of the n-th child
			CColRefSet *PcrsRequiredAgg
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsRequired,
				ULONG ulChildIndex,
				DrgPcr *pdrgpcrGrp
				);

			// compute required distribution of the n-th child
			CDistributionSpec *PdsRequiredAgg
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsInput,
				ULONG ulChildIndex,
				ULONG  ulOptReq,
				DrgPcr *pdrgpcgGrp,
				DrgPcr *pdrgpcrGrpMinimal
				)
				const;

		public:

			// ctor
			CPhysicalAgg
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				DrgPcr *pdrgpcrMinimal, // FD's on grouping columns
				COperator::EGbAggType egbaggtype,
				BOOL fGeneratesDuplicates,
				DrgPcr *pdrgpcrArgDQA,
				BOOL fMultiStage
				);

			// dtor
			virtual
			~CPhysicalAgg();

			// does this aggregate generate duplicate values for the same group
			virtual
			BOOL FGeneratesDuplicates() const
			{
				return m_fGeneratesDuplicates;
			}

			virtual
			const DrgPcr *PdrgpcrGroupingCols() const
			{
				return m_pdrgpcr;
			}

			// array of columns used in distinct qualified aggregates (DQA)
			virtual
			const DrgPcr *PdrgpcrArgDQA() const
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

			// is agg part of multi-stage aggregation
			BOOL FMultiStage() const
			{
				return m_fMultiStage;
			}

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// hash function
			virtual
			ULONG UlHash() const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
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
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				);

			// compute required ctes of the n-th child
			virtual
			CCTEReq *PcteRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CCTEReq *pcter,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

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
				return PdsRequiredAgg(pmp, exprhdl, pdsRequired, ulChildIndex, ulOptReq, m_pdrgpcr, m_pdrgpcrMinimal);
			}

			// compute required rewindability of the n-th child
			virtual
			CRewindabilitySpec *PrsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CRewindabilitySpec *prsRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// check if required columns are included in output columns
			virtual
			BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG ulOptReq) const;

			
			// compute required partition propagation of the n-th child
			virtual
			CPartitionPropagationSpec *PppsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CPartitionPropagationSpec *pppsRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				);
			
			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive distribution
			virtual
			CDistributionSpec *PdsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive rewindability
			virtual
			CRewindabilitySpec *PrsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive
				(
				IMemoryPool *, // pmp
				CExpressionHandle &exprhdl,
				CDrvdPropCtxt * //pdpctxt
				)
				const
			{
				return PpimPassThruOuter(exprhdl);
			}
			
			// derive partition filter map
			virtual
			CPartFilterMap *PpfmDerive
				(
				IMemoryPool *, // pmp
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpfmPassThruOuter(exprhdl);
			}

			//-------------------------------------------------------------------------------------
			// Enforced Properties
			//-------------------------------------------------------------------------------------

			// return distribution property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetDistribution
				(
				CExpressionHandle &exprhdl,
				const CEnfdDistribution *ped
				) 
				const;

			// return rewindability property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetRewindability
				(
				CExpressionHandle &, // exprhdl
				const CEnfdRewindability * // per
				)
				const;

			// return true if operator passes through stats obtained from children,
			// this is used when computing stats during costing
			virtual
			BOOL FPassThruStats() const
			{
				return false;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CPhysicalAgg *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(CUtils::FPhysicalAgg(pop));

				return dynamic_cast<CPhysicalAgg*>(pop);
			}

			// debug print
			virtual 
			IOstream &OsPrint(IOstream &os) const;

	}; // class CPhysicalAgg

}


#endif // !GPOS_CPhysicalAgg_H

// EOF
