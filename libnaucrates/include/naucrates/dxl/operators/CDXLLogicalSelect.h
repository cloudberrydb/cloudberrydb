//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalSelect.h
//
//	@doc:
//		Class for representing DXL logical select operators
//		
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalSelect_H
#define GPDXL_CDXLLogicalSelect_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalSelect
	//
	//	@doc:
	//		Class for representing DXL Logical Select operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalSelect : public CDXLLogical
	{
		private:

			// private copy ctor
			CDXLLogicalSelect(CDXLLogicalSelect&);

		public:
			// ctor/dtor
			explicit
			CDXLLogicalSelect(IMemoryPool *);

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalSelect *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalSelect == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalSelect*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLLogicalSelect_H

// EOF
