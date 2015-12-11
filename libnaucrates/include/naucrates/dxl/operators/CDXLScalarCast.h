//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarCast.h
//
//	@doc:
//		Class for representing DXL casting operation
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCast_H
#define GPDXL_CDXLScalarCast_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarCast
	//
	//	@doc:
	//		Class for representing DXL casting operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarCast : public CDXLScalar
	{

		private:

			// catalog MDId of the column's type
			IMDId *m_pmdidType;

			// catalog MDId of the function implementing the casting
			IMDId *m_pmdidFunc;

			// private copy ctor
			CDXLScalarCast(const CDXLScalarCast&);

		public:
			// ctor/dtor
			CDXLScalarCast
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				IMDId *pmdidOpFunc
				);

			virtual
			~CDXLScalarCast();

			// ident accessors
			Edxlopid Edxlop() const;

			IMDId *PmdidType() const;
			IMDId *PmdidFunc() const;

			// name of the DXL operator name
			const CWStringConst *PstrOpName() const;

			// conversion function
			static
			CDXLScalarCast *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarCast == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarCast*>(pdxlop);
			}

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;
	};
}

#endif // !GPDXL_CDXLScalarCast_H

// EOF
