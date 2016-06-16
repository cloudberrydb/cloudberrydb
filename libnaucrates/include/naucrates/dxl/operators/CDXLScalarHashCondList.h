//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarHashCondList.h
//
//	@doc:
//		Class for representing the list of hash conditions in DXL Hash join nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarHashCondList_H
#define GPDXL_CDXLScalarHashCondList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"


namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarHashCondList
	//
	//	@doc:
	//		Class for representing the list of hash conditions in DXL Hash join nodes.
	//
	//---------------------------------------------------------------------------
	class CDXLScalarHashCondList : public CDXLScalar
	{
		private:
		
			// private copy ctor
			CDXLScalarHashCondList(CDXLScalarHashCondList&);
			
		public:
			// ctor
			explicit
			CDXLScalarHashCondList(IMemoryPool *pmp);
			
			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarHashCondList *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarHashCondList == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarHashCondList*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarHashCondList_H

// EOF
