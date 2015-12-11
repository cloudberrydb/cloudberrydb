//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalSort.h
//
//	@doc:
//		Class for representing DXL sort operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalSort_H
#define GPDXL_CDXLPhysicalSort_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	// indices of sort node elements in the children array
	enum Edxlsort
	{
		EdxlsortIndexProjList = 0,
		EdxlsortIndexFilter,
		EdxlsortIndexSortColList,
		EdxlsortIndexLimitCount,
		EdxlsortIndexLimitOffset,
		EdxlsortIndexChild,
		EdxlsortIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalSort
	//
	//	@doc:
	//		Class for representing DXL sort operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalSort : public CDXLPhysical
	{
		private:
			// private copy ctor
			CDXLPhysicalSort(const CDXLPhysicalSort&);
			
			// whether sort discards duplicates
			BOOL m_fDiscardDuplicates;
			

		public:
			// ctor/dtor
			CDXLPhysicalSort(IMemoryPool *pmp, BOOL fDiscardDuplicates);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			BOOL FDiscardDuplicates() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalSort *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalSort == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalSort*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalSort_H

// EOF

