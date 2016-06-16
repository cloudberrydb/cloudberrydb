//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalJoin.h
//
//	@doc:
//		Class for representing DXL logical Join operators
//		
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalJoin_H
#define GPDXL_CDXLLogicalJoin_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalJoin
	//
	//	@doc:
	//		Class for representing DXL logical Join operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalJoin : public CDXLLogical
	{
		private:

			// private copy ctor
			CDXLLogicalJoin(CDXLLogicalJoin&);

			// join type (inner, outer, ...)
			EdxlJoinType m_edxljt;

		public:
			// ctor/dtor
			CDXLLogicalJoin(IMemoryPool *, 	EdxlJoinType);

			// accessors
			Edxlopid Edxlop() const;

			const CWStringConst *PstrOpName() const;

			// join type
			EdxlJoinType Edxltype() const;

			const CWStringConst *PstrJoinTypeName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalJoin *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalJoin == pdxlop->Edxlop());
				return dynamic_cast<CDXLLogicalJoin*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLLogicalJoin_H

// EOF
