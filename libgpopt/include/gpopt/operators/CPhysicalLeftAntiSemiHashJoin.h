//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftAntiSemiHashJoin.h
//
//	@doc:
//		Left anti semi hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftAntiSemiHashJoin_H
#define GPOPT_CPhysicalLeftAntiSemiHashJoin_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalLeftAntiSemiHashJoin
	//
	//	@doc:
	//		Left anti semi hash join operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalLeftAntiSemiHashJoin : public CPhysicalHashJoin
	{

		private:

			// private copy ctor
			CPhysicalLeftAntiSemiHashJoin(const CPhysicalLeftAntiSemiHashJoin &);

		public:

			// ctor
			CPhysicalLeftAntiSemiHashJoin
				(
				IMemoryPool *mp,
				CExpressionArray *pdrgpexprOuterKeys,
				CExpressionArray *pdrgpexprInnerKeys
				);

			// dtor
			virtual
			~CPhysicalLeftAntiSemiHashJoin();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalLeftAntiSemiHashJoin;
			}

			 // return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalLeftAntiSemiHashJoin";
			}

			// check if required columns are included in output columns
			virtual
			BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG ulOptReq) const;

			// compute required partition propagation spec
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
			
			// conversion function
			static
			CPhysicalLeftAntiSemiHashJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(EopPhysicalLeftAntiSemiHashJoin == pop->Eopid());

				return dynamic_cast<CPhysicalLeftAntiSemiHashJoin*>(pop);
			}


	}; // class CPhysicalLeftAntiSemiHashJoin

}

#endif // !GPOPT_CPhysicalLeftAntiSemiHashJoin_H

// EOF
