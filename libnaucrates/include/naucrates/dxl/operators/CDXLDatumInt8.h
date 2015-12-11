//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumInt8.h
//
//	@doc:
//		Class for representing DXL datums of type long int
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumInt8_H
#define GPDXL_CDXLDatumInt8_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDatumInt8
	//
	//	@doc:
	//		Class for representing DXL datums of type long int
	//
	//---------------------------------------------------------------------------
	class CDXLDatumInt8 : public CDXLDatum
	{
		private:
			// long int value
			LINT m_lVal;

			// private copy ctor
			CDXLDatumInt8(const CDXLDatumInt8 &);

		public:
			// ctor
			CDXLDatumInt8
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				BOOL fNull,
				LINT lVal
				);

			// dtor
			virtual
			~CDXLDatumInt8(){};

			// accessor of value
			LINT LValue() const;

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser);

			// datum type
			virtual
			EdxldatumType Edxldt() const
			{
				return CDXLDatum::EdxldatumInt8;
			}

			// is type passed by value
			virtual
			BOOL FByValue() const
			{
				return true;
			}

			// conversion function
			static
			CDXLDatumInt8 *PdxldatumConvert
				(
				CDXLDatum *pdxldatum
				)
			{
				GPOS_ASSERT(NULL != pdxldatum);
				GPOS_ASSERT(CDXLDatum::EdxldatumInt8 == pdxldatum->Edxldt());

				return dynamic_cast<CDXLDatumInt8*>(pdxldatum);
			}
	};
}

#endif // !GPDXL_CDXLDatumInt8_H

// EOF
