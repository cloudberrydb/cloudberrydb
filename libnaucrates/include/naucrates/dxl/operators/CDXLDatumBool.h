//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumBool.h
//
//	@doc:
//		Class for representing DXL boolean datum
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumBool_H
#define GPDXL_CDXLDatumBool_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDatumBool
	//
	//	@doc:
	//		Class for representing DXL boolean datums
	//
	//---------------------------------------------------------------------------
	class CDXLDatumBool : public CDXLDatum
	{
		private:
			// boolean value
			BOOL m_fVal;

			// private copy ctor
			CDXLDatumBool(const CDXLDatumBool &);

		public:
			// ctor
			CDXLDatumBool
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				BOOL fNull,
				BOOL fVal
				);

			// dtor
			virtual
			~CDXLDatumBool(){}

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser);

			// is type passed by value
			virtual BOOL FByValue() const
			{
				return true;
			}

			// datum type
			virtual
			EdxldatumType Edxldt() const
			{
				return CDXLDatum::EdxldatumBool;
			}

			// accessor of boolean value
			BOOL FValue() const
			{
				return m_fVal;
			}

			// conversion function
			static
			CDXLDatumBool *PdxldatumConvert
				(
				CDXLDatum *pdxldatum
				)
			{
				GPOS_ASSERT(NULL != pdxldatum);
				GPOS_ASSERT(CDXLDatum::CDXLDatum::EdxldatumBool == pdxldatum->Edxldt());

				return dynamic_cast<CDXLDatumBool*>(pdxldatum);
			}
	};
}

#endif // !GPDXL_CDXLDatumBool_H

// EOF
