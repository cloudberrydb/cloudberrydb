//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDynamicIndexScan.h
//
//	@doc:
//		Class for representing DXL dynamic index scan operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalDynamicIndexScan_H
#define GPDXL_CDXLPhysicalDynamicIndexScan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLIndexDescr.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"

namespace gpdxl
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalDynamicIndexScan
	//
	//	@doc:
	//		Class for representing DXL dynamic index scan operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalDynamicIndexScan : public CDXLPhysical
	{
		private:

			// table descriptor for the scanned table
			CDXLTableDescr *m_pdxltabdesc;

			// part index id
			ULONG m_ulPartIndexId;
			
			// printable partition index id
			ULONG m_ulPartIndexIdPrintable;

			// index descriptor associated with the scanned table
			CDXLIndexDescr *m_pdxlid;

			// scan direction of the index
			EdxlIndexScanDirection m_edxlisd;

			// private copy ctor
			CDXLPhysicalDynamicIndexScan(CDXLPhysicalDynamicIndexScan&);

		public:

			// indices of dynamic index scan elements in the children array
			enum Edxldis
			{
				EdxldisIndexProjList = 0,
				EdxldisIndexFilter,
				EdxldisIndexCondition,
				EdxldisSentinel
			};
			
			//ctor
			CDXLPhysicalDynamicIndexScan
				(
				IMemoryPool *pmp,
				CDXLTableDescr *pdxltabdesc,
				ULONG ulPartIndexId,
				ULONG ulPartIndexIdPrintable,
				CDXLIndexDescr *pdxlid,
				EdxlIndexScanDirection edxlisd
				);

			//dtor
			virtual
			~CDXLPhysicalDynamicIndexScan();

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// index descriptor
			const CDXLIndexDescr *Pdxlid() const;

			//table descriptor
			const CDXLTableDescr *Pdxltabdesc() const;

			// partition index id
			ULONG UlPartIndexId() const;
			
			// printable partition index id
			ULONG UlPartIndexIdPrintable() const;

			// scan direction
			EdxlIndexScanDirection EdxlScanDirection() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalDynamicIndexScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalDynamicIndexScan == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalDynamicIndexScan*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLPhysicalDynamicIndexScan_H

// EOF

