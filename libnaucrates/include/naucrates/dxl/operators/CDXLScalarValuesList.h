//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CDXLScalarValuesList.h
//
//	@doc:
//		Class for representing DXL value list operator.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarValuesList_H
#define GPDXL_CDXLScalarValuesList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;


	// class for representing DXL value list operator
	class CDXLScalarValuesList : public CDXLScalar
	{
		private:

			// private copy ctor
			CDXLScalarValuesList(CDXLScalarValuesList&);

		public:

			// ctor
			CDXLScalarValuesList(IMemoryPool *pmp);

			// dtor
			virtual
			~CDXLScalarValuesList();

			// ident accessors
			Edxlopid Edxlop() const;

			// name of the DXL operator
			const CWStringConst *PstrOpName() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLScalarValuesList *PdxlopConvert(CDXLOperator *pdxlop);

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor * /*pmda*/) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
	};
}

#endif // !GPDXL_CDXLScalarValuesList_H

// EOF
