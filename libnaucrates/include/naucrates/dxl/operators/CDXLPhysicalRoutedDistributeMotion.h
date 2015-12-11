//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalRoutedDistributeMotion.h
//
//	@doc:
//		Class for representing DXL routed redistribute motion operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalRoutedDistributeMotion_H
#define GPDXL_CDXLPhysicalRoutedDistributeMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"

namespace gpdxl
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalRoutedDistributeMotion
	//
	//	@doc:
	//		Class for representing DXL routed redistribute motion operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalRoutedDistributeMotion : public CDXLPhysicalMotion
	{
		private:

			// indexes of the routed motion elements in the children array
			enum Edxlroutedm
			{
				EdxlroutedmIndexProjList = 0,
				EdxlroutedmIndexFilter,
				EdxlroutedmIndexSortColList,
				EdxlroutedmIndexChild,
				EdxlroutedmIndexSentinel
			};
		
			// segment id column
			ULONG m_ulSegmentIdCol;
		
			// private copy ctor
			CDXLPhysicalRoutedDistributeMotion(const CDXLPhysicalRoutedDistributeMotion&);
			
		public:
			// ctor
			CDXLPhysicalRoutedDistributeMotion(IMemoryPool *pmp, ULONG ulSegmentIdCol);
			
			// operator type
			Edxlopid Edxlop() const;
			
			// operator name
			const CWStringConst *PstrOpName() const;
			
			// segment id column
			ULONG UlSegmentIdCol() const
			{
				return m_ulSegmentIdCol;
			}
			
			// index of relational child node in the children array
			virtual 
			ULONG UlChildIndex() const
			{
				return EdxlroutedmIndexChild;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalRoutedDistributeMotion *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalMotionRoutedDistribute == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalRoutedDistributeMotion*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalRoutedDistributeMotion_H

// EOF

