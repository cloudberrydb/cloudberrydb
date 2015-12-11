//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarIfStmt.h
//
//	@doc:
//		
//		Class for representing DXL If Statement (Case Statement is represented as a nested if statement)
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarIfStmt_H
#define GPDXL_CDXLScalarIfStmt_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarIfStmt
	//
	//	@doc:
	//		Class for representing DXL IF statement
	//
	//---------------------------------------------------------------------------
	class CDXLScalarIfStmt : public CDXLScalar
	{
		private:
			// catalog MDId of the return type
			IMDId *m_pmdidResultType;

			// private copy ctor
			CDXLScalarIfStmt(const CDXLScalarIfStmt&);

		public:

			// ctor
			CDXLScalarIfStmt
				(
				IMemoryPool *pmp,
				IMDId *pmdidType
				);

			//dtor
			virtual
			~CDXLScalarIfStmt();

			// name of the operator
			const CWStringConst *PstrOpName() const;

			IMDId *PmdidResultType() const;

			// DXL Operator ID
			Edxlopid Edxlop() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarIfStmt *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarIfStmt == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarIfStmt*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}
#endif // !GPDXL_CDXLScalarIfStmt_H

// EOF
