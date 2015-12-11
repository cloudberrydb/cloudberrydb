//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarNullIf.h
//
//	@doc:
//		Class for representing DXL scalar NullIf operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarNullIf_H
#define GPDXL_CDXLScalarNullIf_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarNullIf
	//
	//	@doc:
	//		Class for representing DXL scalar NullIf operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarNullIf : public CDXLScalar
	{

		private:

			// operator number
			IMDId *m_pmdidOp;

			// return type
			IMDId *m_pmdidType;

			// private copy ctor
			CDXLScalarNullIf(CDXLScalarNullIf&);

		public:
			// ctor
			CDXLScalarNullIf
				(
				IMemoryPool *pmp,
				IMDId *pmdidOp,
				IMDId *pmdidType
				);

			// dtor
			virtual
			~CDXLScalarNullIf();

			// ident accessors
			virtual
			Edxlopid Edxlop() const;

			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			// operator id
			virtual
			IMDId *PmdidOp() const;

			// return type
			virtual
			IMDId *PmdidType() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

			// conversion function
			static
			CDXLScalarNullIf *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarNullIf == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarNullIf*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarNullIf_H


// EOF
