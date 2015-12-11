//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarOpExpr.h
//
//	@doc:
//		Class for representing DXL scalar OpExpr such as A.a1 + (B.b1 * C.c1)
//		
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarOpExpr_H
#define GPDXL_CDXLScalarOpExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarOpExpr
	//
	//	@doc:
	//		Class for representing DXL scalar OpExpr
	//
	//---------------------------------------------------------------------------
	class CDXLScalarOpExpr : public CDXLScalar
	{
		private:

			// operator number in the catalog
			IMDId *m_pmdid;
			
			// return type (or invalid if type can be infered from the metadata)
			IMDId *m_pmdidReturnType;

			// operator name
			const CWStringConst *m_pstrOpName;

			// private copy ctor
			CDXLScalarOpExpr(const CDXLScalarOpExpr&);

		public:
			// ctor/dtor
			CDXLScalarOpExpr
				(
				IMemoryPool *pmp,
				IMDId *pmdidOp,
				IMDId *pmdidReturnType,
				const CWStringConst *pstrOpName
				);

			virtual
			~CDXLScalarOpExpr();

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			// name of the operator
			const CWStringConst *PstrScalarOpName() const;

			// operator id
			IMDId *Pmdid() const;
			
			// operator return type
			IMDId *PmdidReturnType() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarOpExpr *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarOpExpr == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarOpExpr*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarOpExpr_H

// EOF
