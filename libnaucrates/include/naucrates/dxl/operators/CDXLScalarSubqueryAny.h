//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryAny.h
//
//	@doc:
//		Class for representing ANY subqueries
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSubqueryAny_H
#define GPDXL_CDXLScalarSubqueryAny_H

#include "gpos/base.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/CMDName.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalarSubqueryQuantified.h"

namespace gpdxl
{

	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarSubqueryAny
	//
	//	@doc:
	//		Class for representing ANY subqueries
	//
	//---------------------------------------------------------------------------
	class CDXLScalarSubqueryAny : public CDXLScalarSubqueryQuantified
	{			

		private:	
			
			// private copy ctor
			CDXLScalarSubqueryAny(CDXLScalarSubqueryAny&);
			
		public:
			// ctor
			CDXLScalarSubqueryAny
				(
				IMemoryPool *pmp,
				IMDId *pmdidScalarOp,
				CMDName *pmdname,
				ULONG ulColId
				);

			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			// conversion function
			static
			CDXLScalarSubqueryAny *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarSubqueryAny == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarSubqueryAny*>(pdxlop);
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
	};
}


#endif // !GPDXL_CDXLScalarSubqueryAny_H

// EOF
