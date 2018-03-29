//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CPhysicalDynamicScan.h
//
//	@doc:
//		Base class for physical dynamic scan operators
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CPhysicalDynamicScan_H
#define GPOPT_CPhysicalDynamicScan_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalScan.h"

namespace gpopt
{
	// fwd declarations
	class CTableDescriptor;
	class CName;
	class CPartConstraint;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalDynamicScan
	//
	//	@doc:
	//		Base class for physical dynamic scan operators
	//
	//---------------------------------------------------------------------------
	class CPhysicalDynamicScan : public CPhysicalScan
	{
		private:
			// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
			ULONG m_ulOriginOpId;

			// true iff it is a partial scan
			BOOL m_fPartial;

			// id of the dynamic scan
			ULONG m_ulScanId;

			// partition keys
			DrgDrgPcr *m_pdrgpdrgpcrPart;

			// secondary scan id in case of partial scan
			ULONG m_ulSecondaryScanId;

			// dynamic index part constraint
			CPartConstraint *m_ppartcnstr;

			// relation part constraint
			CPartConstraint *m_ppartcnstrRel;

			// disable copy ctor
			CPhysicalDynamicScan(const CPhysicalDynamicScan &);

		public:
			// ctor
			CPhysicalDynamicScan
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

			// dtor
			virtual
			~CPhysicalDynamicScan();

			// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
			ULONG UlOriginOpId() const
			{
				return m_ulOriginOpId;
			}

			// true iff the scan is partial
			BOOL FPartial() const
			{
				return m_fPartial;
			}

			// return scan id
			ULONG UlScanId() const
			{
				return m_ulScanId;
			}

			// partition keys
			DrgDrgPcr *PdrgpdrgpcrPart() const
			{
				return m_pdrgpdrgpcrPart;
			}

			// secondary scan id
			ULONG UlSecondaryScanId() const
			{
				return m_ulSecondaryScanId;
			}

			// dynamic index part constraint
			CPartConstraint *Ppartcnstr() const
			{
				return m_ppartcnstr;
			}

			// relation part constraint
			CPartConstraint *PpartcnstrRel() const
			{
				return m_ppartcnstrRel;
			}

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdpctxt) const;

			// return true if operator is dynamic scan
			virtual
			BOOL FDynamicScan() const
			{
				return true;
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

			// conversion function
			static
			CPhysicalDynamicScan *PopConvert(COperator *pop);

	};
}

#endif // !GPOPT_CPhysicalDynamicScan_H

// EOF
