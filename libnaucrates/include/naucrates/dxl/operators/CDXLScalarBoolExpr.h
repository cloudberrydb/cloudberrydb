//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarBoolExpr.h
//
//	@doc:
//		Class for representing DXL BoolExpr
//		
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarBoolExpr_H
#define GPDXL_CDXLScalarBoolExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"


namespace gpdxl
{
	using namespace gpos;

	enum EdxlBoolExprType
		{
			Edxland,
			Edxlor,
			Edxlnot,
			EdxlBoolExprTypeSentinel
		};


	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarBoolExpr
	//
	//	@doc:
	//		Class for representing DXL BoolExpr
	//
	//---------------------------------------------------------------------------
	class CDXLScalarBoolExpr : public CDXLScalar
	{

		private:

			// operator type
			const EdxlBoolExprType m_boolexptype;

			// private copy ctor
			CDXLScalarBoolExpr(const CDXLScalarBoolExpr&);

		public:
			// ctor/dtor
			explicit
			CDXLScalarBoolExpr
				(
				IMemoryPool *pmp,
				const EdxlBoolExprType boolexptype
				);


			// ident accessors
			Edxlopid Edxlop() const;

			// BoolExpr operator type
			EdxlBoolExprType EdxlBoolType() const;

			// name of the DXL operator name
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarBoolExpr *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarBoolExpr == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarBoolExpr*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarBoolExpr_H

// EOF
