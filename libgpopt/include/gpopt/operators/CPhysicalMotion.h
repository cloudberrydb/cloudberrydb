//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalMotion.h
//
//	@doc:
//		Base class for Motion operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalMotion_H
#define GPOPT_CPhysicalMotion_H

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPhysical.h"


namespace gpopt
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalMotion
	//
	//	@doc:
	//		Base class for Motion operators
	//
	//---------------------------------------------------------------------------
	class CPhysicalMotion : public CPhysical
	{
		private:

			// private copy ctor
			CPhysicalMotion(const CPhysicalMotion &);
			
		protected:

			// ctor
			explicit
			CPhysicalMotion
				(
				IMemoryPool *pmp
				)
				:
				CPhysical(pmp)
			{}

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

		public:

			// output distribution accessor
			virtual
			CDistributionSpec *Pds() const = 0;

			// check if optimization contexts is valid
			virtual
			BOOL FValidContext(IMemoryPool *pmp, COptimizationContext *poc, DrgPoc *pdrgpocChild) const;

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

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
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required rewindability of the n-th child
			virtual
			CRewindabilitySpec *PrsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &, // exprhdl
				CRewindabilitySpec *, // prsRequired
				ULONG, // ulChildIndex
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
				return true;
			}

			// conversion function
			static
			CPhysicalMotion *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(CUtils::FPhysicalMotion(pop));

				return dynamic_cast<CPhysicalMotion*>(pop);
			}

	}; // class CPhysicalMotion

}

#endif // !GPOPT_CPhysicalMotion_H

// EOF
