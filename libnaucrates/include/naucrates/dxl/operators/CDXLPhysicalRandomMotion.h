//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalRandomMotion.h
//
//	@doc:
//		Class for representing DXL random motion operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalRandomMotion_H
#define GPDXL_CDXLPhysicalRandomMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"

namespace gpdxl
{

	// indexes of random motion elements in the children array
	enum Edxlrandomm
	{
		EdxlrandommIndexProjList = 0,
		EdxlrandommIndexFilter,
		EdxlrandommIndexSortColList,
		EdxlrandommIndexChild,
		EdxlrandommIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalRandomMotion
	//
	//	@doc:
	//		Class for representing DXL random motion operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalRandomMotion : public CDXLPhysicalMotion
	{
		private:

			// is distribution duplicate sensitive
			BOOL m_fDuplicateSensitive;
			
			// private copy ctor
			CDXLPhysicalRandomMotion(const CDXLPhysicalRandomMotion&);
			
		public:
			// ctor
			CDXLPhysicalRandomMotion(IMemoryPool *pmp, BOOL fDuplicateSensitive);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			// is operator duplicate sensitive
			BOOL FDuplicateSensitive() const
			{
				return m_fDuplicateSensitive;
			}
			
			// index of relational child node in the children array
			virtual 
			ULONG UlChildIndex() const
			{
				return EdxlrandommIndexChild;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalRandomMotion *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalMotionRandom == pdxlop->Edxlop());
				return dynamic_cast<CDXLPhysicalRandomMotion*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalRandomMotion_H

// EOF

