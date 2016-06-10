//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CDXLScalarAssertConstraintList.h
//
//	@doc:
//		Class for representing DXL scalar assert constraint lists for Assert operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarAssertConstraintList_H
#define GPDXL_CDXLScalarAssertConstraintList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarAssertConstraintList
	//
	//	@doc:
	//		Class for representing DXL scalar assert constraint lists
	//
	//---------------------------------------------------------------------------
	class CDXLScalarAssertConstraintList : public CDXLScalar
	{
		private:

			// private copy ctor
			CDXLScalarAssertConstraintList(const CDXLScalarAssertConstraintList&);


		public:
			// ctor
			explicit
			CDXLScalarAssertConstraintList(IMemoryPool *pmp);

			// ident accessors
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

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
			CDXLScalarAssertConstraintList *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarAssertConstraintList == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarAssertConstraintList*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarAssertConstraintList_H

// EOF
