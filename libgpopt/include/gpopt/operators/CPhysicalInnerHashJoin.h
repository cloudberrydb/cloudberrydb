//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalInnerHashJoin.h
//
//	@doc:
//		Inner hash join operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalInnerHashJoin_H
#define GPOPT_CPhysicalInnerHashJoin_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalInnerHashJoin
	//
	//	@doc:
	//		Inner hash join operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalInnerHashJoin : public CPhysicalHashJoin
	{

		private:

			// helper for computing a hashed distribution matching the given distribution
			CDistributionSpecHashed *PdshashedCreateMatching(IMemoryPool *pmp, CDistributionSpecHashed *pdshashed, ULONG ulSourceChild) const;

			// helper for deriving hash join distribution from hashed children
			CDistributionSpec *PdsDeriveFromHashedChildren(IMemoryPool *pmp, CDistributionSpec *pdsOuter, CDistributionSpec *pdsInner) const;

			// helper for deriving hash join distribution from replicated outer child
			CDistributionSpec *PdsDeriveFromReplicatedOuter(IMemoryPool *pmp, CDistributionSpec *pdsOuter, CDistributionSpec *pdsInner) const;

			// helper for deriving hash join distribution from hashed outer child
			CDistributionSpec *PdsDeriveFromHashedOuter(IMemoryPool *pmp, CDistributionSpec *pdsOuter, CDistributionSpec *pdsInner) const;

			// private copy ctor
			CPhysicalInnerHashJoin(const CPhysicalInnerHashJoin &);

		public:

			// ctor
			CPhysicalInnerHashJoin
				(
				IMemoryPool *pmp,
				DrgPexpr *pdrgpexprOuterKeys,
				DrgPexpr *pdrgpexprInnerKeys
				);

			// dtor
			virtual
			~CPhysicalInnerHashJoin();

			// ident accessors

			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalInnerHashJoin;
			}

			 // return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalInnerHashJoin";
			}

			// conversion function
			static
			CPhysicalInnerHashJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(EopPhysicalInnerHashJoin == pop->Eopid());

				return dynamic_cast<CPhysicalInnerHashJoin*>(pop);
			}

			// derive distribution
			virtual
			CDistributionSpec *PdsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

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
			
	}; // class CPhysicalInnerHashJoin

}

#endif // !GPOPT_CPhysicalInnerHashJoin_H

// EOF
