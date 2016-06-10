//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryNotExists.h
//
//	@doc:
//		Class for representing NOT EXISTS subqueries
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSubqueryNotExists_H
#define GPDXL_CDXLScalarSubqueryNotExists_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarSubqueryNotExists
	//
	//	@doc:
	//		Class for representing NOT EXISTS subqueries
	//
	//---------------------------------------------------------------------------
	class CDXLScalarSubqueryNotExists : public CDXLScalar
	{
		private:			
			// private copy ctor
			CDXLScalarSubqueryNotExists(CDXLScalarSubqueryNotExists&);
			
		public:
			// ctor/dtor
			explicit
			CDXLScalarSubqueryNotExists(IMemoryPool *pmp);
			
			virtual
			~CDXLScalarSubqueryNotExists();

			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarSubqueryNotExists *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarSubqueryNotExists == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarSubqueryNotExists*>(pdxlop);
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


#endif // !GPDXL_CDXLScalarSubqueryNotExists_H

// EOF
