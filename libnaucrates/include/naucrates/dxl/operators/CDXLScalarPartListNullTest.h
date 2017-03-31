//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Class for representing DXL Part list null test expressions
//	These expressions indicate whether the list values of a part
//	contain NULL value or not
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarPartListNullTest_H
#define GPDXL_CDXLScalarPartListNullTest_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{

	class CDXLScalarPartListNullTest : public CDXLScalar
	{
		private:

			// partitioning level
			ULONG m_ulLevel;

			// Null Test type (true for 'is null', false for 'is not null')
			BOOL m_fIsNull;

			// private copy ctor
			CDXLScalarPartListNullTest(const CDXLScalarPartListNullTest&);

		public:
			// ctor
			CDXLScalarPartListNullTest(IMemoryPool *pmp, ULONG ulLevel, BOOL fLower);

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// partitioning level
			ULONG UlLevel() const;

			// Null Test type (true for 'is null', false for 'is not null')
			BOOL FIsNull() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLScalarPartListNullTest *PdxlopConvert(CDXLOperator *pdxlop);
	};
}

#endif // !GPDXL_CDXLScalarPartListNullTest_H

// EOF
