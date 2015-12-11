//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalResult.h
//
//	@doc:
//		Class for representing DXL physical result operators.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalResult_H
#define GPDXL_CDXLPhysicalResult_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	// indices of result elements in the children array
	enum Edxlresult
	{
		EdxlresultIndexProjList = 0,
		EdxlresultIndexFilter,
		EdxlresultIndexOneTimeFilter,
		EdxlresultIndexChild,
		EdxlresultIndexSentinel
	};
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalResult
	//
	//	@doc:
	//		Class for representing DXL result operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalResult : public CDXLPhysical
	{
		private:


			// private copy ctor
			CDXLPhysicalResult(CDXLPhysicalResult&);

		public:
			// ctor/dtor
			explicit
			CDXLPhysicalResult(IMemoryPool *pmp);

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalResult *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalResult == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalResult*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLPhysicalResult_H

// EOF

