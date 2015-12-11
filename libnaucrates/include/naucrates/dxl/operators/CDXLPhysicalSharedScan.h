//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalSharedScan.h
//
//	@doc:
//		Class for representing DXL physical shared scan operators.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalSharedScan_H
#define GPDXL_CDXLPhysicalSharedScan_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLSpoolInfo.h"


namespace gpdxl
{
	// indices of shared scan elements in the children array
	enum Edxlshscan
	{
		EdxlshscanIndexProjList = 0,
		EdxlshscanIndexFilter,
		EdxlshscanIndexChild,
		EdxlshscanIndexSentinel
	};
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalSharedScan
	//
	//	@doc:
	//		Class for representing DXL shared scan operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalSharedScan : public CDXLPhysical
	{
		private:
			// spool info
			CDXLSpoolInfo *m_pspoolinfo;
			
			// private copy ctor
			CDXLPhysicalSharedScan(CDXLPhysicalSharedScan&);

		public:
			// ctor/dtor
			CDXLPhysicalSharedScan(IMemoryPool *pmp, CDXLSpoolInfo *pspoolinfo);
			~CDXLPhysicalSharedScan();
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			const CDXLSpoolInfo *Pspoolinfo() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalSharedScan *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalSharedScan == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalSharedScan*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLPhysicalSharedScan_H

// EOF

