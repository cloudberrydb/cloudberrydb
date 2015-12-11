//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalAppend.h
//
//	@doc:
//		Class for representing DXL append operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalAppend_H
#define GPDXL_CDXLPhysicalAppend_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	// indices of append elements in the children array
	enum Edxlappend
	{
		EdxlappendIndexProjList = 0,
		EdxlappendIndexFilter,
		EdxlappendIndexFirstChild,
		EdxlappendIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalAppend
	//
	//	@doc:
	//		Class for representing DXL Append operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalAppend : public CDXLPhysical
	{
		private:
			// is the append node used in an update/delete statement
			BOOL m_fIsTarget;
			
			// TODO:  - Apr 12, 2011; find a better name (and comments) for this variable
			BOOL m_fIsZapped;
			
			// private copy ctor
			CDXLPhysicalAppend(const CDXLPhysicalAppend&);
			
		public:
			// ctor/dtor
			CDXLPhysicalAppend(IMemoryPool *pmp, BOOL fIsTarget, BOOL fIsZapped);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			BOOL FIsTarget() const;
			BOOL FIsZapped() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalAppend *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalAppend == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalAppend*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalAppend_H

// EOF

