//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CDXLScalarArrayCoerceExpr.h
//
//	@doc:
//		Class for representing DXL ArrayCoerceExpr operation,
//		the operator will apply type casting for each element in this array
//		using the given element coercion function.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarArrayCoerceExpr_H
#define GPDXL_CDXLScalarArrayCoerceExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarArrayCoerceExpr
	//
	//	@doc:
	//		Class for representing DXL array coerce operator
	//---------------------------------------------------------------------------
	class CDXLScalarArrayCoerceExpr : public CDXLScalar
	{
		private:
			// catalog MDId of element coerce function
			IMDId *m_pmdidElementFunc;
		
			// catalog MDId of the result type
			IMDId *m_pmdidResultType;

			// output type modifications
			INT m_iMod;

			// conversion semantics flag to pass to func
			BOOL m_fIsExplicit;

			// coercion form
			EdxlCoercionForm m_edxlcf;

			// location of token to be coerced
			INT m_iLoc;

			// private copy ctor
			CDXLScalarArrayCoerceExpr(const CDXLScalarArrayCoerceExpr&);
		
		public:
			CDXLScalarArrayCoerceExpr
				(
				IMemoryPool *pmp,
				IMDId *pmdidElementFunc,
				IMDId *pmdidResultType,
				INT iMod,
				BOOL fIsExplicit,
				EdxlCoercionForm edxlcf,
				INT iLoc
				);
		
			~CDXLScalarArrayCoerceExpr();
		
			// ident accessor
			virtual
			Edxlopid Edxlop() const
			{
				return EdxlopScalarArrayCoerceExpr;
			}

		
			// return metadata id of element coerce function
			IMDId *PmdidElementFunc() const
			{
				return m_pmdidElementFunc;
			}
		
			// return result type
			IMDId *PmdidResultType() const
			{
				return m_pmdidResultType;
			}

			// return type modification
			INT IMod() const
			{
				return m_iMod;
			}

			BOOL FIsExplicit() const
			{
				return m_fIsExplicit;
			}
		
			// return coercion form
			EdxlCoercionForm Edxlcf() const
			{
				return m_edxlcf;
			}

			// return token location
			INT ILoc() const
			{
				return m_iLoc;
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

			// name of the DXL operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarArrayCoerceExpr *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarArrayCoerceExpr == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarArrayCoerceExpr*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarArrayCoerceExpr_H

// EOF
