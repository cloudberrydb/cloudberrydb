//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSortColList.h
//
//	@doc:
//		Class for representing a list of sorting columns in a DXL Sort and Motion nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSortColList_H
#define GPDXL_CDXLScalarSortColList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarSortColList
	//
	//	@doc:
	//		Sorting column lists in DXL Sort And Motion nodes
	//
	//---------------------------------------------------------------------------
	class CDXLScalarSortColList : public CDXLScalar
	{
		private:
		
			// private copy ctor
			CDXLScalarSortColList(CDXLScalarSortColList&);
			
		public:
			// ctor/dtor
			explicit
			CDXLScalarSortColList(IMemoryPool *pmp);
			
			virtual
			~CDXLScalarSortColList(){};

			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarSortColList *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarSortColList == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarSortColList*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarSortColList_H

// EOF
