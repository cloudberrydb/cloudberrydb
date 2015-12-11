//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysicalHashAgg.h
//
//	@doc:
//		Hash Aggregate operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CPhysicalHashAgg_H
#define GPOS_CPhysicalHashAgg_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalAgg.h"

namespace gpopt
{
	// fwd declaration
	class CDistributionSpec;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalHashAgg
	//
	//	@doc:
	//		Hash-based aggregate operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalHashAgg : public CPhysicalAgg
	{
		private:

			// private copy ctor
			CPhysicalHashAgg(const CPhysicalHashAgg &);

		public:

			// ctor
			CPhysicalHashAgg
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr,
				DrgPcr *pdrgpcrMinimal,
				COperator::EGbAggType egbaggtype,
				BOOL fGeneratesDuplicates,
				DrgPcr *pdrgpcrArgDQA,
				BOOL fMultiStage
				);

			// dtor
			virtual
			~CPhysicalHashAgg();


			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopPhysicalHashAgg;
			}

			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CPhysicalHashAgg";
			}

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

			// compute required sort columns of the n-th child
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
		
			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive sort order
			virtual
			COrderSpec *PosDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

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

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CPhysicalHashAgg *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalHashAgg == pop->Eopid() ||
						EopPhysicalHashAggDeduplicate == pop->Eopid());

				return reinterpret_cast<CPhysicalHashAgg*>(pop);
			}
		
	}; // class CPhysicalHashAgg

}


#endif // !GPOS_CPhysicalHashAgg_H

// EOF
