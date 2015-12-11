//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalIndexScan.h
//
//	@doc:
//		Class for representing DXL index scan operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalIndexScan_H
#define GPDXL_CDXLPhysicalIndexScan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLIndexDescr.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"

namespace gpdxl
{
	// indices of index scan elements in the children array
	enum Edxlis
	{
		EdxlisIndexProjList = 0,
		EdxlisIndexFilter,
		EdxlisIndexCondition,
		EdxlisSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalIndexScan
	//
	//	@doc:
	//		Class for representing DXL index scan operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalIndexScan : public CDXLPhysical
	{
		private:

			// table descriptor for the scanned table
			CDXLTableDescr *m_pdxltabdesc;

			// index descriptor associated with the scanned table
			CDXLIndexDescr *m_pdxlid;

			// scan direction of the index
			EdxlIndexScanDirection m_edxlisd;

			// private copy ctor
			CDXLPhysicalIndexScan(CDXLPhysicalIndexScan&);

		public:

			//ctor
			CDXLPhysicalIndexScan
				(
				IMemoryPool *pmp,
				CDXLTableDescr *pdxltabdesc,
				CDXLIndexDescr *pdxlid,
				EdxlIndexScanDirection edxlisd
				);

			//dtor
			virtual
			~CDXLPhysicalIndexScan();

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// index descriptor
			virtual
			const CDXLIndexDescr *Pdxlid() const;

			//table descriptor
			virtual
			const CDXLTableDescr *Pdxltabdesc() const;

			// scan direction
			virtual
			EdxlIndexScanDirection EdxlScanDirection() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalIndexScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalIndexScan == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalIndexScan*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLPhysicalIndexScan_H

// EOF

