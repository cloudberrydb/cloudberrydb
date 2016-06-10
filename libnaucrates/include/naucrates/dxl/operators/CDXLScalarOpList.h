//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarOpList.h
//
//	@doc:
//		Class for representing DXL list of scalar expressions
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarOpList_H
#define GPDXL_CDXLScalarOpList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarOpList
	//
	//	@doc:
	//		Class for representing DXL list of scalar expressions
	//
	//---------------------------------------------------------------------------
	class CDXLScalarOpList : public CDXLScalar
	{
		public:

			// type of the operator list
			enum EdxlOpListType
				{
					EdxloplistEqFilterList,
					EdxloplistFilterList,
					EdxloplistGeneral,
					EdxloplistSentinel
				};

		private:

			// operator list type
			EdxlOpListType m_edxloplisttype;

			// private copy ctor
			CDXLScalarOpList(const CDXLScalarOpList&);

		public:
			// ctor
			CDXLScalarOpList(IMemoryPool *pmp, EdxlOpListType edxloplisttype = EdxloplistGeneral);

			// operator type
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
			CDXLScalarOpList *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarOpList == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarOpList*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarOpList_H

// EOF
