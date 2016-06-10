//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalHashJoin.h
//
//	@doc:
//		Class for representing DXL hashjoin operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalHashJoin_H
#define GPDXL_CDXLPhysicalHashJoin_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysicalJoin.h"

namespace gpdxl
{
	// indices of hash join elements in the children array
	enum Edxlhj
	{
		EdxlhjIndexProjList = 0,
		EdxlhjIndexFilter,
		EdxlhjIndexJoinFilter,
		EdxlhjIndexHashCondList,
		EdxlhjIndexHashLeft,
		EdxlhjIndexHashRight,
		EdxlhjIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalHashJoin
	//
	//	@doc:
	//		Class for representing DXL hashjoin operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalHashJoin : public CDXLPhysicalJoin
	{
		private:
			// private copy ctor
			CDXLPhysicalHashJoin(const CDXLPhysicalHashJoin&);

		public:
			// ctor/dtor
			CDXLPhysicalHashJoin(IMemoryPool *pmp, EdxlJoinType edxljt);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalHashJoin *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalHashJoin == pdxlop->Edxlop());
				return dynamic_cast<CDXLPhysicalHashJoin*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalHashJoin_H

// EOF

