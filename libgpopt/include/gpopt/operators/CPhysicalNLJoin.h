//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalNLJoin.h
//
//	@doc:
//		Base nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalNLJoin_H
#define GPOPT_CPhysicalNLJoin_H

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPhysicalJoin.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalNLJoin
	//
	//	@doc:
	//		Inner nested-loops join operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalNLJoin : public CPhysicalJoin
	{

		private:

			// private copy ctor
			CPhysicalNLJoin(const CPhysicalNLJoin &);

		protected:
			
			// helper function for computing the required partition propagation 
			// spec for the children of a nested loop join
			CPartitionPropagationSpec *PppsRequiredNLJoinChild
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CPartitionPropagationSpec *pppsRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				);
			
		public:

			// ctor
			explicit
			CPhysicalNLJoin(IMemoryPool *pmp);

			// dtor
			virtual
			~CPhysicalNLJoin();

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

			// compute required sort order of the n-th child
			virtual
			COrderSpec *PosRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				COrderSpec *posInput,
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

			// compute required output columns of the n-th child
			virtual
			CColRefSet *PcrsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsRequired,
				ULONG ulChildIndex,
				DrgPdp *, // pdrgpdpCtxt
				ULONG // ulOptReq
				);
			
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
				)
			{
				GPOS_ASSERT(ulOptReq < UlPartPropagateRequests());

				return PppsRequiredNLJoinChild(pmp, exprhdl, pppsRequired, ulChildIndex, pdrgpdpCtxt, ulOptReq);
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
				) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// return true if operator is a correlated NL Join
			virtual
			BOOL FCorrelated() const
			{
				return false;
			}

			// return required inner columns -- overloaded by correlated join children
			virtual
			DrgPcr *PdrgPcrInner() const
			{
				return NULL;
			}

			// conversion function
			static
			CPhysicalNLJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(CUtils::FNLJoin(pop));

				return dynamic_cast<CPhysicalNLJoin*>(pop);
			}


	}; // class CPhysicalNLJoin

}

#endif // !GPOPT_CPhysicalNLJoin_H

// EOF
