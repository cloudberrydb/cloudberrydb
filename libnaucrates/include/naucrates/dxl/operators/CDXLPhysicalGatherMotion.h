//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalGatherMotion.h
//
//	@doc:
//		Class for representing DXL Gather motion operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalGatherMotion_H
#define GPDXL_CDXLPhysicalGatherMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"

namespace gpdxl
{
	// indices of gather motion elements in the children array
	enum Edxlgm
	{
		EdxlgmIndexProjList = 0,
		EdxlgmIndexFilter,
		EdxlgmIndexSortColList,
		EdxlgmIndexChild,
		EdxlgmIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalGatherMotion
	//
	//	@doc:
	//		Class for representing DXL gather motion operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalGatherMotion : public CDXLPhysicalMotion
	{
		private:
			
			// private copy ctor
			CDXLPhysicalGatherMotion(const CDXLPhysicalGatherMotion&);
			
		public:
			// ctor/dtor
			CDXLPhysicalGatherMotion(IMemoryPool *pmp);
			
			virtual
			~CDXLPhysicalGatherMotion(){};

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			INT IOutputSegIdx() const;
			
			// index of relational child node in the children array
			virtual 
			ULONG UlChildIndex() const
			{
				return EdxlgmIndexChild;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalGatherMotion *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalMotionGather == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalGatherMotion*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}

#endif // !GPDXL_CDXLPhysicalGatherMotion_H

// EOF

