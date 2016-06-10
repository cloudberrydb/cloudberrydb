//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalUnionAll.h
//
//	@doc:
//		Physical union all operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalUnionAll_H
#define GPOPT_CPhysicalUnionAll_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{
	// fwd declaration
	class CDistributionSpecHashed;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalUnionAll
	//
	//	@doc:
	//		Physical union all operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalUnionAll : public CPhysical
	{

		private:

			// output column array
			DrgPcr *m_pdrgpcrOutput;

			// input column array
			DrgDrgPcr *m_pdrgpdrgpcrInput;

			// set representation of input columns
			DrgPcrs *m_pdrgpcrsInput;

			// array of child hashed distributions -- used locally for distribution derivation
			DrgPds *m_pdrgpds;

			// if this union is needed for partial indexes then store the scan
			// id, otherwise this will be ULONG_MAX
			ULONG m_ulScanIdPartialIndex;

			// map given array of scalar ident expressions to positions of UnionAll input columns in the given child;
			DrgPul *PdrgpulMap(IMemoryPool *pmp, DrgPexpr *pdrgpexpr, ULONG ulChildIndex) const;

			// compute required hashed distribution of the n-th child
			CDistributionSpecHashed *PdshashedPassThru(IMemoryPool *pmp, CDistributionSpecHashed *pdshashedRequired, ULONG ulChildIndex) const;

			// derive hashed distribution from child operators
			CDistributionSpecHashed *PdshashedDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// compute output hashed distribution matching the outer child's hashed distribution
			CDistributionSpecHashed *PdsMatching(IMemoryPool *pmp, const DrgPul *pdrgpulOuter) const;

			// derive output distribution based on child distribution
			CDistributionSpec *PdsDeriveFromChildren(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// build hashed distributions used in property derivation
			void BuildHashedDistributions(IMemoryPool *pmp);

			// helper to do value equality check of arrays of ULONG pointers
			static
			BOOL FEqual(DrgPul *pdrgpulFst, DrgPul *pdrgpulSnd);

#ifdef GPOS_DEBUG
			// helper to assert distribution delivered by UnionAll children
			static
			void AssertValidChildDistributions
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CDistributionSpec::EDistributionType *pedt, // array of distribution types to check
				ULONG ulDistrs, // number of distribution types to check
				const CHAR *szAssertMsg
				);

			// helper to check if UnionAll children have valid distributions
			static
			void CheckChildDistributions
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				BOOL fSingletonChild,
				BOOL fReplicatedChild,
				BOOL fUniversalOuterChild
				);
#endif // GPOS_DEBUG

			// private copy ctor
			CPhysicalUnionAll(const CPhysicalUnionAll &);

		public:

			// ctor
			CPhysicalUnionAll
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrOutput,
				DrgDrgPcr *pdrgpdrgpcrInput,
				ULONG ulScanIdPartialIndex
				);

			// dtor
			virtual
			~CPhysicalUnionAll();

			// accessor of output column array
			DrgPcr *PdrgpcrOutput() const
			{
				return m_pdrgpcrOutput;
			}

			// accessor of input column array
			DrgDrgPcr *PdrgpdrgpcrInput() const
			{
				return m_pdrgpdrgpcrInput;
			}

			// if this unionall is needed for partial indexes then return the scan
			// id, otherwise return ULONG_MAX
			ULONG UlScanIdPartialIndex() const
			{
				return m_ulScanIdPartialIndex;
			}

			// is this unionall needed for a partial index
			BOOL FPartialIndex() const
			{
				return (ULONG_MAX > m_ulScanIdPartialIndex);
			}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalUnionAll;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalUnionAll";
			}

			// match function
			virtual
			BOOL FMatch(COperator *) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return false;
			}

			// distribution matching type
			virtual
			CEnfdDistribution::EDistributionMatching Edm
				(
				CReqdPropPlan *prppInput,
				ULONG , // ulChildIndex
				DrgPdp *, //pdrgpdpCtxt
				ULONG ulOptReq
				)
			{
				if (0 == ulOptReq  && CDistributionSpec::EdtHashed == prppInput->Ped()->PdsRequired()->Edt())
				{
					// use exact matching if optimizing first request
					return CEnfdDistribution::EdmExact;
				}

				// use relaxed matching if optimizing other requests
				return CEnfdDistribution::EdmSatisfy;
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

			// compute required sort order of the n-th child
			virtual
			COrderSpec *PosRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				COrderSpec *posRequired,
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
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

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
			
			// check if required columns are included in output columns
			virtual
			BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG ulOptReq) const;


			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive sort order
			virtual
			COrderSpec *PosDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive distribution
			virtual
			CDistributionSpec *PdsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive rewindability
			virtual
			CRewindabilitySpec *PrsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdpctxt) const;
			
			// derive partition filter map
			virtual
			CPartFilterMap *PpfmDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				// combine part filter maps from relational children
				return PpfmDeriveCombineRelational(pmp, exprhdl);
			}

			//-------------------------------------------------------------------------------------
			// Enforced Properties
			//-------------------------------------------------------------------------------------

			// return order property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetOrder
				(
				CExpressionHandle &exprhdl,
				const CEnfdOrder *peo
				) 
				const;

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

			// return partition propagation property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetPartitionPropagation
				(
				CExpressionHandle &exprhdl,
				const CEnfdPartitionPropagation *pepp
				) 
				const;
			
			// return true if operator passes through stats obtained from children,
			// this is used when computing stats during costing
			virtual
			BOOL FPassThruStats() const
			{
				return false;
			}

			// conversion function
			static
			CPhysicalUnionAll *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalUnionAll == pop->Eopid());

				return dynamic_cast<CPhysicalUnionAll*>(pop);
			}

	}; // class CPhysicalUnionAll

}

#endif // !GPOPT_CPhysicalUnionAll_H

// EOF
