//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarProjList.h
//
//	@doc:
//		Class for representing DXL projection lists.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarProjList_H
#define GPDXL_CDXLScalarProjList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"


namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarProjList
	//
	//	@doc:
	//		Projection list in operators.
	//
	//---------------------------------------------------------------------------
	class CDXLScalarProjList : public CDXLScalar
	{
		private:
		
			// private copy ctor
			CDXLScalarProjList(CDXLScalarProjList&);
			
		public:
			// ctor/dtor
			explicit
			CDXLScalarProjList(IMemoryPool *pmp);
			
			virtual
			~CDXLScalarProjList(){};

			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarProjList *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarProjectList == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarProjList*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarProjList_H

// EOF
