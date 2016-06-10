//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDynamicTableScan.h
//
//	@doc:
//		Class for representing DXL dynamic table scan operators
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalDynamicTableScan_H
#define GPDXL_CDXLPhysicalDynamicTableScan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"


namespace gpdxl
{
	// indices of dynamic table scan elements in the children array
	enum Edxldts
	{
		EdxldtsIndexProjList = 0,
		EdxldtsSentinel
	};
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalDynamicTableScan
	//
	//	@doc:
	//		Class for representing DXL dynamic table scan operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalDynamicTableScan : public CDXLPhysical
	{
		private:
		
			// table descriptor for the scanned table
			CDXLTableDescr *m_pdxltabdesc;
			
			// id of partition index structure
			ULONG m_ulPartIndexId;
			
			// printable partition index id
			ULONG m_ulPartIndexIdPrintable;

			// private copy ctor
			CDXLPhysicalDynamicTableScan(CDXLPhysicalDynamicTableScan&);

		public:
			// ctor
			CDXLPhysicalDynamicTableScan
				(
				IMemoryPool *pmp,
				CDXLTableDescr *pdxltabdesc,
				ULONG ulPartIndexId,
				ULONG ulPartIndexIdPrintable
				);
			
			// dtor
			virtual
			~CDXLPhysicalDynamicTableScan();
			
			// operator type
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// table descriptor
			const CDXLTableDescr *Pdxltabdesc() const;

			// partition index id
			ULONG UlPartIndexId() const;
			
			// printable partition index id
			ULONG UlPartIndexIdPrintable() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalDynamicTableScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalDynamicTableScan == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalDynamicTableScan*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalDynamicTableScan_H

// EOF

