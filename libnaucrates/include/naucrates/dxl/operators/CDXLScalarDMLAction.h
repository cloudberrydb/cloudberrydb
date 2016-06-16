//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarDMLAction.h
//
//	@doc:
//		Class for representing DXL DML action expressions
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarDMLAction_H
#define GPDXL_CDXLScalarDMLAction_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarDMLAction
	//
	//	@doc:
	//		Class for representing DXL DML action expressions
	//
	//---------------------------------------------------------------------------
	class CDXLScalarDMLAction : public CDXLScalar
	{
		private:

			// private copy ctor
			CDXLScalarDMLAction(const CDXLScalarDMLAction&);

		public:
			// ctor/dtor
			explicit
			CDXLScalarDMLAction(IMemoryPool *pmp);

			virtual
			~CDXLScalarDMLAction(){}

			// ident accessors
			Edxlopid Edxlop() const;

			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarDMLAction *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarDMLAction == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarDMLAction*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarDMLAction_H

// EOF
