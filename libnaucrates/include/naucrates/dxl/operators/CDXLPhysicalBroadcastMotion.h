//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalBroadcastMotion.h
//
//	@doc:
//		Class for representing DXL Broadcast motion operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalBroadcastMotion_H
#define GPDXL_CDXLPhysicalBroadcastMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"

namespace gpdxl
{
	// indices of broadcast motion elements in the children array
	enum Edxlbm
	{
		EdxlbmIndexProjList = 0,
		EdxlbmIndexFilter,
		EdxlbmIndexSortColList,
		EdxlbmIndexChild,
		EdxlbmIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalBroadcastMotion
	//
	//	@doc:
	//		Class for representing DXL broadcast motion operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalBroadcastMotion : public CDXLPhysicalMotion
	{
		private:
			
			// private copy ctor
			CDXLPhysicalBroadcastMotion(const CDXLPhysicalBroadcastMotion&);
			
		public:
			// ctor/dtor
			explicit CDXLPhysicalBroadcastMotion(IMemoryPool *pmp);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			// index of relational child node in the children array
			virtual 
			ULONG UlChildIndex() const
			{
				return EdxlbmIndexChild;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalBroadcastMotion *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalMotionBroadcast == pdxlop->Edxlop());
				return dynamic_cast<CDXLPhysicalBroadcastMotion*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalBroadcastMotion_H

// EOF

