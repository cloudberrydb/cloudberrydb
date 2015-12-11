//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarIndexCondList.h
//
//	@doc:
//		Class for representing the list of index conditions in DXL index scan
//		operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarIndexCondList_H
#define GPDXL_CDXLScalarIndexCondList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarIndexCondList
	//
	//	@doc:
	//		Class for representing the list of index conditions in DXL index scan
	// 		operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarIndexCondList : public CDXLScalar
	{
		private:

			// private copy ctor
			CDXLScalarIndexCondList(CDXLScalarIndexCondList&);

		public:

			// ctor
			explicit
			CDXLScalarIndexCondList(IMemoryPool *pmp);

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the operator
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarIndexCondList *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarIndexCondList == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarIndexCondList*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				GPOS_ASSERT(!"Invalid function call on a container operator");
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}

#endif // !GPDXL_CDXLScalarIndexCondList_H

// EOF
