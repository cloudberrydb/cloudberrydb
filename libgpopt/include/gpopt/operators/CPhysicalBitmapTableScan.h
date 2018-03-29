//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CPhysicalBitmapTableScan.h
//
//	@doc:
//		Bitmap table scan physical operator
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CPhysicalBitmapTableScan_H
#define GPOPT_CPhysicalBitmapTableScan_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalScan.h"

namespace gpopt
{
	// fwd declarations
	class CDistributionSpec;
	class CTableDescriptor;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalBitmapTableScan
	//
	//	@doc:
	//		Bitmap table scan physical operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalBitmapTableScan : public CPhysicalScan
	{
		private:
			// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
			ULONG m_ulOriginOpId;

			// disable copy ctor
			CPhysicalBitmapTableScan(const CPhysicalBitmapTableScan &);

		public:
			// ctor
			CPhysicalBitmapTableScan
				(
				IMemoryPool *pmp,
				CTableDescriptor *ptabdesc,
				ULONG ulOriginOpId,
				const CName *pnameTableAlias,
				DrgPcr *pdrgpcrOutput
				);

			// dtor
			virtual
			~CPhysicalBitmapTableScan()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalBitmapTableScan;
			}

			// operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalBitmapTableScan";
			}

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
			ULONG UlOriginOpId() const
			{
				return m_ulOriginOpId;
			}

			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

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
				GPOS_ASSERT(!"stats derivation during costing for bitmap table scan is invalid");

				return NULL;
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

			// conversion function
			static
			CPhysicalBitmapTableScan *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalBitmapTableScan == pop->Eopid());

				return dynamic_cast<CPhysicalBitmapTableScan *>(pop);
			}

	};
}

#endif // !GPOPT_CPhysicalBitmapTableScan_H

// EOF
