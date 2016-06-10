//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalRedistributeMotion.h
//
//	@doc:
//		Class for representing DXL Redistribute motion operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalRedistributeMotion_H
#define GPDXL_CDXLPhysicalRedistributeMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"

namespace gpdxl
{
	// indices of redistribute motion elements in the children array
	enum Edxlrm
	{
		EdxlrmIndexProjList = 0,
		EdxlrmIndexFilter,
		EdxlrmIndexSortColList,
		EdxlrmIndexHashExprList,
		EdxlrmIndexChild,
		EdxlrmIndexSentinel
	};


	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalRedistributeMotion
	//
	//	@doc:
	//		Class for representing DXL redistribute motion operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalRedistributeMotion : public CDXLPhysicalMotion
	{
		private:
			
			// is this a duplicate sensitive redistribute motion
			BOOL m_fDuplicateSensitive;
			
			// private copy ctor
			CDXLPhysicalRedistributeMotion(const CDXLPhysicalRedistributeMotion&);
			
			
		public:
			// ctor
			CDXLPhysicalRedistributeMotion(IMemoryPool *pmp, BOOL fDuplicateSensitive);
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			// does motion remove duplicates
			BOOL FDuplicateSensitive() const
			{
				return m_fDuplicateSensitive;
			}
			
			// index of relational child node in the children array
			virtual 
			ULONG UlChildIndex() const
			{
				return EdxlrmIndexChild;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalRedistributeMotion *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalMotionRedistribute == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalRedistributeMotion*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalRedistributeMotion_H

// EOF

