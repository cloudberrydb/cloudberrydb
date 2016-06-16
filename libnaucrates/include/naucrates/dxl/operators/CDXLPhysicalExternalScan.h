//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalExternalScan.h
//
//	@doc:
//		Class for representing DXL external scan operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalExternalScan_H
#define GPDXL_CDXLPhysicalExternalScan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysicalTableScan.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalExternalScan
	//
	//	@doc:
	//		Class for representing DXL external scan operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalExternalScan : public CDXLPhysicalTableScan
	{
		private:

			// private copy ctor
			CDXLPhysicalExternalScan(CDXLPhysicalExternalScan&);

		public:
			// ctors
			explicit
			CDXLPhysicalExternalScan(IMemoryPool *pmp);

			CDXLPhysicalExternalScan(IMemoryPool *pmp, CDXLTableDescr *pdxltabdesc);

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// conversion function
			static
			CDXLPhysicalExternalScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalExternalScan == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalExternalScan*>(pdxlop);
			}

	};
}
#endif // !GPDXL_CDXLPhysicalExternalScan_H

// EOF

