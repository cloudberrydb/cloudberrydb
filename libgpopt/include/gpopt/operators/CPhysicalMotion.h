//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalMotion.h
//
//	@doc:
//		Base class for Motion operators
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
				IMemoryPool *mp
				)
				:
				CPhysical(mp)
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
			BOOL FValidContext(IMemoryPool *mp, COptimizationContext *poc, COptimizationContextArray *pdrgpocChild) const;

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

			// compute required ctes of the n-th child
			virtual
			CCTEReq *PcteRequired
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				CCTEReq *pcter,
				ULONG child_index,
				CDrvdProp2dArray *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required distribution of the n-th child
			virtual
			CDistributionSpec *PdsRequired
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsRequired,
				ULONG child_index,
				CDrvdProp2dArray *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required rewindability of the n-th child
			virtual
			CRewindabilitySpec *PrsRequired
				(
				IMemoryPool *mp,
				CExpressionHandle &, // exprhdl
				CRewindabilitySpec *, // prsRequired
				ULONG, // child_index
				CDrvdProp2dArray *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required partition propagation of the n-th child
			virtual
			CPartitionPropagationSpec *PppsRequired
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				CPartitionPropagationSpec *pppsRequired,
				ULONG child_index,
				CDrvdProp2dArray *pdrgpdpCtxt,
				ULONG ulOptReq
				);
			
			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive distribution
			virtual
			CDistributionSpec *PdsDerive(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive rewindability
			virtual
			CRewindabilitySpec *PrsDerive(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive
				(
				IMemoryPool *, // mp
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
				IMemoryPool *, // mp
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
