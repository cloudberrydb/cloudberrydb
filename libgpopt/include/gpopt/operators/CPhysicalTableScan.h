//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysicalTableScan.h
//
//	@doc:
//		Table scan operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalTableScan_H
#define GPOPT_CPhysicalTableScan_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalScan.h"

namespace gpopt
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalTableScan
	//
	//	@doc:
	//		Table scan operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalTableScan : public CPhysicalScan
	{

		private:

			// private copy ctor
			CPhysicalTableScan(const CPhysicalTableScan&);

		public:
		
			// ctors
			explicit
			CPhysicalTableScan(IMemoryPool *pmp);
			CPhysicalTableScan(IMemoryPool *, const CName *, CTableDescriptor *, DrgPcr *);

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopPhysicalTableScan;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CPhysicalTableScan";
			}
			
			// operator specific hash function
			virtual ULONG UlHash() const;
			
			// match function
			BOOL FMatch(COperator *) const;
		
			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &, // exprhdl
				CDrvdPropCtxt * //pdpctxt
				)
				const
			{
				return GPOS_NEW(pmp) CPartIndexMap(pmp);
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// debug print
			virtual 
			IOstream &OsPrint(IOstream &) const;


			// conversion function
			static
			CPhysicalTableScan *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalTableScan == pop->Eopid() ||
							EopPhysicalExternalScan == pop->Eopid());

				return reinterpret_cast<CPhysicalTableScan*>(pop);
			}

			// statistics derivation during costing
			virtual
			IStatistics *PstatsDerive
				(
				IMemoryPool *, // pmp
				CExpressionHandle &, // exprhdl
				CReqdPropPlan *, // prpplan
				DrgPstat * //pdrgpstatCtxt
				)
				const
			{
				GPOS_ASSERT(!"stats derivation during costing for table scan is invalid");

				return NULL;
			}


	}; // class CPhysicalTableScan

}

#endif // !GPOPT_CPhysicalTableScan_H

// EOF
