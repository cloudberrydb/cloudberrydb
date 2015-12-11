//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEAnchor.h
//
//	@doc:
//		Class for representing DXL logical CTE anchors
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalCTEAnchor_H
#define GPDXL_CDXLLogicalCTEAnchor_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalCTEAnchor
	//
	//	@doc:
	//		Class for representing DXL logical CTE producers
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalCTEAnchor : public CDXLLogical
	{
		private:

			// cte id
			ULONG m_ulId;
			
			// private copy ctor
			CDXLLogicalCTEAnchor(CDXLLogicalCTEAnchor&);

		public:
			// ctor
			CDXLLogicalCTEAnchor(IMemoryPool *pmp, ULONG ulId);
			
			// operator type
			Edxlopid Edxlop() const;

			// operator name
			const CWStringConst *PstrOpName() const;

			// cte identifier
			ULONG UlId() const
			{
				return m_ulId;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;


#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLLogicalCTEAnchor *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalCTEAnchor == pdxlop->Edxlop());
				return dynamic_cast<CDXLLogicalCTEAnchor*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLLogicalCTEAnchor_H

// EOF
