//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarArrayComp.h
//
//	@doc:
//		Class for representing DXL scalar array comparison such as in, not in, any, all
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarArrayComp_H
#define GPDXL_CDXLScalarArrayComp_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalarComp.h"


namespace gpdxl
{
	using namespace gpos;

	enum EdxlArrayCompType
	{
		Edxlarraycomptypeany = 0,
		Edxlarraycomptypeall
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarArrayComp
	//
	//	@doc:
	//		Class for representing DXL scalar Array OpExpr
	//
	//---------------------------------------------------------------------------
	class CDXLScalarArrayComp : public CDXLScalarComp
	{
		private:

			EdxlArrayCompType m_edxlcomptype;

			// private copy ctor
			CDXLScalarArrayComp(const CDXLScalarArrayComp&);

			const CWStringConst* PstrArrayCompType() const;

		public:
			// ctor/dtor
			CDXLScalarArrayComp
				(
				IMemoryPool *pmp,
				IMDId *pmdidOp,
				const CWStringConst *pstrOpName,
				EdxlArrayCompType edxlcomptype
				);

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			//accessors
			BOOL FBoolean() const;
			EdxlArrayCompType Edxlarraycomptype() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarArrayComp *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarArrayComp == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarArrayComp*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarArrayComp_H

// EOF
