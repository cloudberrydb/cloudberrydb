//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalSort.h
//
//	@doc:
//		Physical sort operator
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalSort_H
#define GPOS_CPhysicalSort_H

#include "gpos/base.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/operators/CPhysical.h"


namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalSort
	//
	//	@doc:
	//		Sort operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalSort : public CPhysical
	{
		private:

			// order spec
			COrderSpec *m_pos;

			// columns used by order spec
			CColRefSet *m_pcrsSort;

			// private copy ctor
			CPhysicalSort(const CPhysicalSort &);

		public:

			// ctor
			CPhysicalSort(IMemoryPool *mp, COrderSpec *pos);

			// dtor
			virtual
			~CPhysicalSort();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalSort;
			}

			// sort order accessor
			virtual
			const COrderSpec *Pos() const
			{
				return m_pos;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalSort";
			}

			// match function
			BOOL Matches(COperator *pop) const;

			// sensitivity to order of inputs
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
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsRequired,
				ULONG child_index,
				CDrvdProp2dArray *pdrgpdpCtxt,
				ULONG ulOptReq
				);

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

			// compute required sort order of the n-th child
			virtual
			COrderSpec *PosRequired
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				COrderSpec *posRequired,
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
				CExpressionHandle &exprhdl,
				CRewindabilitySpec *prsRequired,
				ULONG child_index,
				CDrvdProp2dArray *pdrgpdpCtxt,
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
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				CPartitionPropagationSpec *pppsRequired,
				ULONG child_index,
				CDrvdProp2dArray *pdrgpdpCtxt,
				ULONG ulOptReq
				);
			
			// distribution matching type
			virtual
			CEnfdDistribution::EDistributionMatching Edm
				(
				CReqdPropPlan *prppInput,
				ULONG,  // child_index
				CDrvdProp2dArray *, //pdrgpdpCtxt
				ULONG // ulOptReq
				)
			{
				// Sort does not require Motions to be enforced on top,
				// we need to pass down incoming matching type
				return prppInput->Ped()->Edm();
			}

			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive sort order
			virtual
			COrderSpec *PosDerive(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

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
				CExpressionHandle &exprhdl,
				const CEnfdRewindability *per
				)
				const;

			// return true if operator passes through stats obtained from children,
			// this is used when computing stats during costing
			virtual
			BOOL FPassThruStats() const
			{
				return true;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// debug print
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// conversion function
			static
			CPhysicalSort *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalSort == pop->Eopid());

				return dynamic_cast<CPhysicalSort*>(pop);
			}

	}; // class CPhysicalSort

}


#endif // !GPOS_CPhysicalSort_H

// EOF
