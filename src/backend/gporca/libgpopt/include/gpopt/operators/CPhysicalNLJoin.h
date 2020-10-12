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
protected:
	// helper function for computing the required partition propagation
	// spec for the children of a nested loop join
	CPartitionPropagationSpec *PppsRequiredNLJoinChild(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

public:
	CPhysicalNLJoin(const CPhysicalNLJoin &) = delete;

	// ctor
	explicit CPhysicalNLJoin(CMemoryPool *mp);

	// dtor
	~CPhysicalNLJoin() override;

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required sort order of the n-th child
	COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							COrderSpec *posInput, ULONG child_index,
							CDrvdPropArray *pdrgpdpCtxt,
							ULONG ulOptReq) const override;

	// compute required rewindability of the n-th child
	CRewindabilitySpec *PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									CRewindabilitySpec *prsRequired,
									ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const override;

	// compute required output columns of the n-th child
	CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 CColRefSet *pcrsRequired, ULONG child_index,
							 CDrvdPropArray *,	// pdrgpdpCtxt
							 ULONG				// ulOptReq
							 ) override;

	// compute required partition propagation of the n-th child
	CPartitionPropagationSpec *
	PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
				 CPartitionPropagationSpec *pppsRequired, ULONG child_index,
				 CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) override
	{
		GPOS_ASSERT(ulOptReq < UlPartPropagateRequests());

		return PppsRequiredNLJoinChild(mp, exprhdl, pppsRequired, child_index,
									   pdrgpdpCtxt, ulOptReq);
	}

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// return true if operator is a correlated NL Join
	virtual BOOL
	FCorrelated() const
	{
		return false;
	}

	// return required inner columns -- overloaded by correlated join children
	virtual CColRefArray *
	PdrgPcrInner() const
	{
		return NULL;
	}

	// conversion function
	static CPhysicalNLJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(CUtils::FNLJoin(pop));

		return dynamic_cast<CPhysicalNLJoin *>(pop);
	}


};	// class CPhysicalNLJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalNLJoin_H

// EOF
