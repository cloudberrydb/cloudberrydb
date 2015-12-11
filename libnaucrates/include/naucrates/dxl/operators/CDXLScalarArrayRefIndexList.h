//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarArrayRefIndexList.h
//
//	@doc:
//		Class for representing DXL scalar arrayref index list
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarArrayRefIndexList_H
#define GPDXL_CDXLScalarArrayRefIndexList_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarArrayRefIndexList
	//
	//	@doc:
	//		Class for representing DXL scalar arrayref index list
	//
	//---------------------------------------------------------------------------
	class CDXLScalarArrayRefIndexList : public CDXLScalar
	{
		public:

			enum EIndexListBound
			{
				EilbLower,		// lower index
				EilbUpper,		// upper index
				EilbSentinel
			};

		private:

			// index list bound
			EIndexListBound m_eilb;

			// private copy ctor
			CDXLScalarArrayRefIndexList(const CDXLScalarArrayRefIndexList&);

			// string representation of index list bound
			static
			const CWStringConst *PstrIndexListBound(EIndexListBound eilb);

		public:
			// ctor
			CDXLScalarArrayRefIndexList
				(
				IMemoryPool *pmp,
				EIndexListBound eilt
				);

			// ident accessors
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// index list bound
			EIndexListBound Eilb() const
			{
				return m_eilb;
			}

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
			CDXLScalarArrayRefIndexList *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarArrayRefIndexList == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarArrayRefIndexList*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarArrayRefIndexList_H

// EOF
