//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarBooleanTest.h
//
//	@doc:
//		Class for representing DXL BooleanTest
//		
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarBooleanTest_H
#define GPDXL_CDXLScalarBooleanTest_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"


namespace gpdxl
{
	using namespace gpos;

	enum EdxlBooleanTestType
		{
			EdxlbooleantestIsTrue,
			EdxlbooleantestIsNotTrue,
			EdxlbooleantestIsFalse,
			EdxlbooleantestIsNotFalse,
			EdxlbooleantestIsUnknown,
			EdxlbooleantestIsNotUnknown,
			EdxlbooleantestSentinel
		};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarBooleanTest
	//
	//	@doc:
	//		Class for representing DXL BooleanTest
	//
	//---------------------------------------------------------------------------
	class CDXLScalarBooleanTest : public CDXLScalar
	{

		private:

			// operator type
			const EdxlBooleanTestType m_edxlbooleantesttype;

			// private copy ctor
			CDXLScalarBooleanTest(const CDXLScalarBooleanTest&);

			// name of the DXL operator name
			const CWStringConst *PstrOpName() const;

		public:
			// ctor/dtor
			CDXLScalarBooleanTest
				(
				IMemoryPool *pmp,
				const EdxlBooleanTestType edxlboolexptesttype
				);

			// ident accessors
			Edxlopid Edxlop() const;

			// BooleanTest operator type
			EdxlBooleanTestType EdxlBoolType() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarBooleanTest *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarBooleanTest == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarBooleanTest*>(pdxlop);
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

#endif // !GPDXL_CDXLScalarBooleanTest_H

// EOF
