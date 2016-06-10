//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalDynamicTableScan.h
//
//	@doc:
//		Dynamic Table scan operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalDynamicTableScan_H
#define GPOPT_CPhysicalDynamicTableScan_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalDynamicScan.h"

namespace gpopt
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalDynamicTableScan
	//
	//	@doc:
	//		Dynamic Table scan operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalDynamicTableScan : public CPhysicalDynamicScan
	{

		private:
			
			// private copy ctor
			CPhysicalDynamicTableScan(const CPhysicalDynamicTableScan&);

		public:
		
			// ctors
			CPhysicalDynamicTableScan
				(
				IMemoryPool *pmp,
				BOOL fPartial,
				const CName *pname, 
				CTableDescriptor *ptabdesc,
				ULONG ulOriginOpId,
				ULONG ulScanId,
				DrgPcr *pdrgpcr,
				DrgDrgPcr *pdrgpdrgpcrParts,
				ULONG ulSecondaryScanId,
				CPartConstraint *ppartcnstr,
				CPartConstraint *ppartcnstrRel
				);
			
			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopPhysicalDynamicTableScan;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CPhysicalDynamicTableScan";
			}

			// match function
			virtual
			BOOL FMatch(COperator *) const;

			// statistics derivation during costing
			virtual
			IStatistics *PstatsDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl, CReqdPropPlan *prpplan, DrgPstat *pdrgpstatCtxt) const;

			// conversion function
			static
			CPhysicalDynamicTableScan *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalDynamicTableScan == pop->Eopid());

				return dynamic_cast<CPhysicalDynamicTableScan*>(pop);
			}

	}; // class CPhysicalDynamicTableScan

}

#endif // !GPOPT_CPhysicalDynamicTableScan_H

// EOF
