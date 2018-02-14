//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarFuncExpr.h
//
//	@doc:
//		Class for representing DXL scalar FuncExpr
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarFuncExpr_H
#define GPDXL_CDXLScalarFuncExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarFuncExpr
	//
	//	@doc:
	//		Class for representing DXL scalar FuncExpr
	//
	//---------------------------------------------------------------------------
	class CDXLScalarFuncExpr : public CDXLScalar
	{
		private:

			// catalog id of the function
			IMDId *m_pmdidFunc;

			// return type
			IMDId *m_pmdidRetType;

			const INT m_iRetTypeModifier;

			// does the func return a set
			BOOL m_fReturnSet;

			// private copy ctor
			CDXLScalarFuncExpr(const CDXLScalarFuncExpr&);

		public:
			// ctor
			CDXLScalarFuncExpr
				(
				IMemoryPool *pmp,
				IMDId *pmdidFunc,
				IMDId *pmdidRetType,
				INT iRetTypeModifier,
				BOOL fretset
				);

			//dtor
			virtual
			~CDXLScalarFuncExpr();

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			// function id
			IMDId *PmdidFunc() const;

			// return type
			IMDId *PmdidRetType() const;

			INT ITypeModifier() const;

			// does function return a set
			BOOL FReturnSet() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarFuncExpr *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarFuncExpr == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarFuncExpr*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarFuncExpr_H

// EOF
