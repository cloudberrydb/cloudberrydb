//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryExists.h
//
//	@doc:
//		Class for representing EXISTS subqueries
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSubqueryExists_H
#define GPDXL_CDXLScalarSubqueryExists_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarSubqueryExists
	//
	//	@doc:
	//		Class for representing EXISTS subqueries
	//
	//---------------------------------------------------------------------------
	class CDXLScalarSubqueryExists : public CDXLScalar
	{
		private:			
			// private copy ctor
			CDXLScalarSubqueryExists(CDXLScalarSubqueryExists&);
			
		public:
			// ctor/dtor
			explicit
			CDXLScalarSubqueryExists(IMemoryPool *pmp);
			
			virtual
			~CDXLScalarSubqueryExists();

			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarSubqueryExists *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarSubqueryExists == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarSubqueryExists*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				return true;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}

#endif // !GPDXL_CDXLScalarSubqueryExists_H

// EOF
