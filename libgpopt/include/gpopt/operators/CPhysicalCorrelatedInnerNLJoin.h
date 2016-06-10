//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalCorrelatedInnerNLJoin.h
//
//	@doc:
//		Physical Inner Correlated NLJ operator capturing correlated execution
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalCorrelatedInnerNLJoin_H
#define GPOPT_CPhysicalCorrelatedInnerNLJoin_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalInnerNLJoin.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalCorrelatedInnerNLJoin
	//
	//	@doc:
	//		Physical inner NLJ operator capturing correlated execution
	//
	//---------------------------------------------------------------------------
	class CPhysicalCorrelatedInnerNLJoin : public CPhysicalInnerNLJoin
	{

		private:

			// columns from inner child used in correlated execution
			DrgPcr *m_pdrgpcrInner;

			// origin subquery id
			EOperatorId m_eopidOriginSubq;

			// private copy ctor
			CPhysicalCorrelatedInnerNLJoin(const CPhysicalCorrelatedInnerNLJoin &);

		public:

			// ctor
			CPhysicalCorrelatedInnerNLJoin
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrInner,
				EOperatorId eopidOriginSubq
				)
				:
				CPhysicalInnerNLJoin(pmp),
				m_pdrgpcrInner(pdrgpcrInner),
				m_eopidOriginSubq(eopidOriginSubq)
			{
				GPOS_ASSERT(NULL != pdrgpcrInner);

				SetDistrRequests(UlDistrRequestsForCorrelatedJoin());
				GPOS_ASSERT(0 < UlDistrRequests());
			}

			// dtor
			virtual
			~CPhysicalCorrelatedInnerNLJoin()
			{
				m_pdrgpcrInner->Release();
			}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalCorrelatedInnerNLJoin;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalCorrelatedInnerNLJoin";
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
					return m_pdrgpcrInner->FEqual(CPhysicalCorrelatedInnerNLJoin::PopConvert(pop)->PdrgPcrInner());
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

			// conversion function
			static
			CPhysicalCorrelatedInnerNLJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalCorrelatedInnerNLJoin == pop->Eopid());

				return dynamic_cast<CPhysicalCorrelatedInnerNLJoin*>(pop);
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

	}; // class CPhysicalCorrelatedInnerNLJoin

}


#endif // !GPOPT_CPhysicalCorrelatedInnerNLJoin_H

// EOF
