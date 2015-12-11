//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarMergeCondList.h
//
//	@doc:
//		Class for representing the list of merge conditions in DXL Merge join nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarMergeCondList_H
#define GPDXL_CDXLScalarMergeCondList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarMergeCondList
	//
	//	@doc:
	//		Class for representing the list of merge conditions in DXL Merge join nodes.
	//
	//---------------------------------------------------------------------------
	class CDXLScalarMergeCondList : public CDXLScalar
	{
		private:
		
			// private copy ctor
			CDXLScalarMergeCondList(CDXLScalarMergeCondList&);
			
		public:
			// ctor
			explicit
			CDXLScalarMergeCondList(IMemoryPool *pmp);
			
			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarMergeCondList *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarMergeCondList == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarMergeCondList*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				GPOS_ASSERT(!"Invalid function call for a container operator");
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}

#endif // !GPDXL_CDXLScalarMergeCondList_H

// EOF
