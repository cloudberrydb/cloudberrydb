//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarHashExpr.h
//
//	@doc:
//		Class for representing hash expressions list in DXL Redistribute motion nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarHashExpr_H
#define GPDXL_CDXLScalarHashExpr_H

#include "gpos/base.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarHashExpr
	//
	//	@doc:
	//		Hash expressions list in Redistribute motion nodes
	//
	//---------------------------------------------------------------------------
	class CDXLScalarHashExpr : public CDXLScalar
	{
		private:
		
			// catalog Oid of the expressions's data type 
			IMDId *m_pmdidType;
			
			// private copy ctor
			CDXLScalarHashExpr(CDXLScalarHashExpr&);
			
		public:
			// ctor/dtor
			CDXLScalarHashExpr(IMemoryPool *pmp, IMDId *pmdidType);
			
			virtual
			~CDXLScalarHashExpr();

			// ident accessors
			Edxlopid Edxlop() const;
			
			// name of the operator
			const CWStringConst *PstrOpName() const;
			
			IMDId *PmdidType() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarHashExpr *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarHashExpr == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarHashExpr*>(pdxlop);
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
			// checks whether the operator has valid structure
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarHashExpr_H

// EOF
