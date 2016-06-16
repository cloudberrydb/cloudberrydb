//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CDXLScalarAssertConstraint.h
//
//	@doc:
//		Class for representing DXL scalar assert constraints
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarAssertConstraint_H
#define GPDXL_CDXLScalarAssertConstraint_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarAssertConstraint
	//
	//	@doc:
	//		Class for representing individual DXL scalar assert constraints
	//
	//---------------------------------------------------------------------------
	class CDXLScalarAssertConstraint : public CDXLScalar
	{
		private:

			// error message
			CWStringBase *m_pstrErrorMsg;
			
			// private copy ctor
			CDXLScalarAssertConstraint(const CDXLScalarAssertConstraint&);

		public:
			// ctor
			CDXLScalarAssertConstraint(IMemoryPool *pmp, CWStringBase *pstrErrorMsg);

			// dtor
			virtual
			~CDXLScalarAssertConstraint();
			
			// ident accessors
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// error message
			CWStringBase *PstrErrorMsg() const;
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
				(
				CMDAccessor * //pmda
				)
				const
			{
				return false;
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLScalarAssertConstraint *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarAssertConstraint == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarAssertConstraint*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarAssertConstraint_H

// EOF
