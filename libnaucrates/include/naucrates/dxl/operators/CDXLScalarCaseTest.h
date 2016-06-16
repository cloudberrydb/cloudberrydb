//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarCaseTest.h
//
//	@doc:
//
//		Class for representing a DXL case test
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCaseTest_H
#define GPDXL_CDXLScalarCaseTest_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarCaseTest
	//
	//	@doc:
	//		Class for representing DXL Case Test
	//
	//---------------------------------------------------------------------------
	class CDXLScalarCaseTest : public CDXLScalar
	{
		private:

			// expression type
			IMDId *m_pmdidType;

			// private copy ctor
			CDXLScalarCaseTest(const CDXLScalarCaseTest&);

		public:

			// ctor
			CDXLScalarCaseTest(IMemoryPool *pmp, IMDId *pmdidType);

			// dtor
			virtual
			~CDXLScalarCaseTest();

			// name of the operator
			virtual
			const CWStringConst *PstrOpName() const;

			// expression type
			virtual
			IMDId *PmdidType() const;

			// DXL Operator ID
			virtual
			Edxlopid Edxlop() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

			// conversion function
			static
			CDXLScalarCaseTest *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarCaseTest == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarCaseTest*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}
#endif // !GPDXL_CDXLScalarCaseTest_H

// EOF
