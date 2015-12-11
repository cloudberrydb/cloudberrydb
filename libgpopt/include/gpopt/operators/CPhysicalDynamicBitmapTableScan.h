//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CPhysicalDynamicBitmapTableScan.h
//
//	@doc:
//		Dynamic bitmap table scan physical operator
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CPhysicalDynamicBitmapTableScan_H
#define GPOPT_CPhysicalDynamicBitmapTableScan_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalDynamicScan.h"

namespace gpopt
{
	// fwd declarations
	class CTableDescriptor;
	class CName;
	class CPartConstraint;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalDynamicBitmapTableScan
	//
	//	@doc:
	//		Dynamic bitmap table scan physical operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalDynamicBitmapTableScan : public CPhysicalDynamicScan
	{
		private:

			// disable copy ctor
			CPhysicalDynamicBitmapTableScan(const CPhysicalDynamicBitmapTableScan &);

		public:
			// ctor
			CPhysicalDynamicBitmapTableScan
				(
				IMemoryPool *pmp,
				BOOL fPartial,
				CTableDescriptor *ptabdesc,
				ULONG ulOriginOpId,
				const CName *pnameAlias,
				ULONG ulScanId,
				DrgPcr *pdrgpcrOutput,
				DrgDrgPcr *pdrgpdrgpcrParts,
				ULONG ulSecondaryScanId,
				CPartConstraint *ppartcnstr,
				CPartConstraint *ppartcnstrRel
				);

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalDynamicBitmapTableScan;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalDynamicBitmapTableScan";
			}

			// match function
			virtual
			BOOL FMatch(COperator *) const;

			// statistics derivation during costing
			virtual
			IStatistics *PstatsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl, CReqdPropPlan *prpplan, DrgPstat *pdrgpstatCtxt) const;

			// conversion function
			static
			CPhysicalDynamicBitmapTableScan *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalDynamicBitmapTableScan == pop->Eopid());

				return dynamic_cast<CPhysicalDynamicBitmapTableScan *>(pop);
			}
	};
}

#endif // !GPOPT_CPhysicalDynamicBitmapTableScan_H

// EOF
