//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPhysicalCorrelatedLeftSemiNLJoin.h
//
//	@doc:
//		Physical Left Semi NLJ operator capturing correlated execution
//		for EXISTS subqueries
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalCorrelatedLeftSemiNLJoin_H
#define GPOPT_CPhysicalCorrelatedLeftSemiNLJoin_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalLeftSemiNLJoin.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalCorrelatedLeftSemiNLJoin
	//
	//	@doc:
	//		Physical left semi NLJ operator capturing correlated execution for
	//		EXISTS subqueries
	//
	//---------------------------------------------------------------------------
	class CPhysicalCorrelatedLeftSemiNLJoin : public CPhysicalLeftSemiNLJoin
	{

		private:

			// columns from inner child used in correlated execution
			DrgPcr *m_pdrgpcrInner;

			// origin subquery id
			EOperatorId m_eopidOriginSubq;

			// private copy ctor
			CPhysicalCorrelatedLeftSemiNLJoin(const CPhysicalCorrelatedLeftSemiNLJoin &);

		public:

			// ctor
			CPhysicalCorrelatedLeftSemiNLJoin
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrInner,
				EOperatorId eopidOriginSubq
				)
				:
				CPhysicalLeftSemiNLJoin(pmp),
				m_pdrgpcrInner(pdrgpcrInner),
				m_eopidOriginSubq(eopidOriginSubq)
			{
				GPOS_ASSERT(NULL != pdrgpcrInner);

				SetDistrRequests(UlDistrRequestsForCorrelatedJoin());
				GPOS_ASSERT(0 < UlDistrRequests());
			}

			// dtor
			virtual
			~CPhysicalCorrelatedLeftSemiNLJoin()
			{
				m_pdrgpcrInner->Release();
			}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalCorrelatedLeftSemiNLJoin;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalCorrelatedLeftSemiNLJoin";
			}

			// match function
			virtual
			BOOL FMatch
				(
				COperator *pop
				)
				const
			{
				if (pop->Eopid() == Eopid())
				{
					return m_pdrgpcrInner->FEqual(CPhysicalCorrelatedLeftSemiNLJoin::PopConvert(pop)->PdrgPcrInner());
				}

				return false;
			}

			// distribution matching type
			virtual
			CEnfdDistribution::EDistributionMatching Edm
				(
				CReqdPropPlan *, // prppInput
				ULONG,  // ulChildIndex
				DrgPdp *, //pdrgpdpCtxt
				ULONG // ulOptReq
				)
			{
				return CEnfdDistribution::EdmSatisfy;
			}

			// compute required distribution of the n-th child
			virtual
			CDistributionSpec *PdsRequired
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsRequired,
				ULONG ulChildIndex,
				DrgPdp *pdrgpdpCtxt,
				ULONG  ulOptReq
				)
				const
			{
				return PdsRequiredCorrelatedJoin(pmp, exprhdl, pdsRequired, ulChildIndex, pdrgpdpCtxt, ulOptReq);
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
				const
			{
				return PrsRequiredCorrelatedJoin(pmp, exprhdl, prsRequired, ulChildIndex, pdrgpdpCtxt, ulOptReq);
			}

			// return true if operator is a correlated NL Join
			virtual
			BOOL FCorrelated() const
			{
				return true;
			}

			// return required inner columns
			virtual
			DrgPcr *PdrgPcrInner() const
			{
				return m_pdrgpcrInner;
			}

			// origin subquery id
			EOperatorId EopidOriginSubq() const
			{
				return m_eopidOriginSubq;
			}

			// print
			virtual
			IOstream &OsPrint
				(
				IOstream &os
				)
				const
			{
				os << this->SzId() << "(";
				(void) CUtils::OsPrintDrgPcr(os, m_pdrgpcrInner);
				os << ")";

				return os;
			}

			// conversion function
			static
			CPhysicalCorrelatedLeftSemiNLJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalCorrelatedLeftSemiNLJoin == pop->Eopid());

				return dynamic_cast<CPhysicalCorrelatedLeftSemiNLJoin*>(pop);
			}

	}; // class CPhysicalCorrelatedLeftSemiNLJoin

}


#endif // !GPOPT_CPhysicalCorrelatedLeftSemiNLJoin_H

// EOF
