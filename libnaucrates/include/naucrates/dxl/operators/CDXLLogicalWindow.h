//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalWindow.h
//
//	@doc:
//		Class for representing DXL logical window operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogicalWindow_H
#define GPDXL_CDXLLogicalWindow_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLLogical.h"
#include "naucrates/dxl/operators/CDXLWindowSpec.h"

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalWindow
	//
	//	@doc:
	//		Class for representing DXL logical window operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalWindow : public CDXLLogical
	{
		private:
			// array of window specifications
			DrgPdxlws *m_pdrgpdxlws;

			// private copy ctor
			CDXLLogicalWindow(CDXLLogicalWindow&);

		public:

			//ctor
			CDXLLogicalWindow(IMemoryPool *pmp, DrgPdxlws *pdrgpdxlwinspec);

			//dtor
			virtual
			~CDXLLogicalWindow();

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;

			// number of window specs
			ULONG UlWindowSpecs() const;

			// return the window key at a given position
			CDXLWindowSpec *Pdxlws(ULONG ulPos) const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalWindow *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalWindow == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalWindow*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLLogicalWindow_H

// EOF

