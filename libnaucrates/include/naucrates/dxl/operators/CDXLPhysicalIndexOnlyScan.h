//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalIndexOnlyScan.h
//
//	@doc:
//		Class for representing DXL index only scan operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalIndexOnlyScan_H
#define GPDXL_CDXLPhysicalIndexOnlyScan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysicalIndexScan.h"
#include "naucrates/dxl/operators/CDXLIndexDescr.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalIndexOnlyScan
	//
	//	@doc:
	//		Class for representing DXL index only scan operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalIndexOnlyScan : public CDXLPhysicalIndexScan
	{
		private:

			// private copy ctor
			CDXLPhysicalIndexOnlyScan(CDXLPhysicalIndexOnlyScan&);

		public:

			//ctor
			CDXLPhysicalIndexOnlyScan
				(
				IMemoryPool *pmp,
				CDXLTableDescr *pdxltabdesc,
				CDXLIndexDescr *pdxlid,
				EdxlIndexScanDirection edxlisd
				);

			//dtor
			virtual
			~CDXLPhysicalIndexOnlyScan()
			{}

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// conversion function
			static
			CDXLPhysicalIndexOnlyScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalIndexOnlyScan == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalIndexOnlyScan*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLPhysicalIndexOnlyScan_H

// EOF

