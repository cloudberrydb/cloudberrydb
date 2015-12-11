//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarNullTest.h
//
//	@doc:
//		Class for representing DXL NullTest that tests if the given expression
//		is null or is not null.
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarNullTest_H
#define GPDXL_CDXLScalarNullTest_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"


namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarNullTest
	//
	//	@doc:
	//		Class for representing DXL NullTest
	//
	//---------------------------------------------------------------------------
	class CDXLScalarNullTest : public CDXLScalar
	{

		private:
			// is nul or is not null operation
			BOOL m_fIsNull;

			// private copy ctor
			CDXLScalarNullTest(const CDXLScalarNullTest&);

		public:
			// ctor/
			CDXLScalarNullTest
				(
				IMemoryPool *pmp,
				BOOL fIsNull
				);

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the DXL operator name
			const CWStringConst *PstrOpName() const;

			// NullTest operator type
			BOOL FIsNullTest() const;

			// name of the operator
			const CWStringConst *PstrTestName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *, const CDXLNode *) const;

			// conversion function
			static
			CDXLScalarNullTest *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarNullTest == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarNullTest*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarNullTest_H

// EOF
