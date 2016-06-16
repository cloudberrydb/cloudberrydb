//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalWindow.h
//
//	@doc:
//		Class for representing DXL window operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalWindow_H
#define GPDXL_CDXLPhysicalWindow_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLWindowKey.h"

namespace gpdxl
{
	// indices of window elements in the children array
	enum Edxlwindow
	{
		EdxlwindowIndexProjList = 0,
		EdxlwindowIndexFilter,
		EdxlwindowIndexChild,
		EdxlwindowIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalWindow
	//
	//	@doc:
	//		Class for representing DXL window operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalWindow : public CDXLPhysical
	{
		private:

			// partition columns
			DrgPul *m_pdrgpulPartCols;

			// window keys
			DrgPdxlwk *m_pdrgpdxlwk;

			// private copy ctor
			CDXLPhysicalWindow(CDXLPhysicalWindow&);

		public:

			//ctor
			CDXLPhysicalWindow(IMemoryPool *pmp, DrgPul *pdrgpulPartCols, DrgPdxlwk *pdrgpdxlwk);

			//dtor
			virtual
			~CDXLPhysicalWindow();

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;

			// number of partition columns
			ULONG UlPartCols() const;

			// return partition columns
			const DrgPul *PrgpulPartCols() const
			{
				return m_pdrgpulPartCols;
			}

			// number of window keys
			ULONG UlWindowKeys() const;

			// return the window key at a given position
			CDXLWindowKey *PdxlWindowKey(ULONG ulPos) const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalWindow *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalWindow == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalWindow*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLPhysicalWindow_H

// EOF

