//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumInt4.h
//
//	@doc:
//		Class for representing DXL integer datum
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumInt4_H
#define GPDXL_CDXLDatumInt4_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDatumInt4
	//
	//	@doc:
	//		Class for representing DXL integer datums
	//
	//---------------------------------------------------------------------------
	class CDXLDatumInt4 : public CDXLDatum
	{
		private:
			// int4 value
			INT m_iVal;

			// private copy ctor
			CDXLDatumInt4(const CDXLDatumInt4 &);

		public:
			// ctor
			CDXLDatumInt4
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				BOOL fNull,
				INT iVal
				);

			// dtor
			virtual
			~CDXLDatumInt4(){};

			// accessor of int value
			INT IValue() const;

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser);

			// datum type
			virtual
			EdxldatumType Edxldt() const
			{
				return CDXLDatum::EdxldatumInt4;
			}

			// is type passed by value
			virtual
			BOOL FByValue() const
			{
				return true;
			}

			// conversion function
			static
			CDXLDatumInt4 *PdxldatumConvert
				(
				CDXLDatum *pdxldatum
				)
			{
				GPOS_ASSERT(NULL != pdxldatum);
				GPOS_ASSERT(CDXLDatum::EdxldatumInt4 == pdxldatum->Edxldt());

				return dynamic_cast<CDXLDatumInt4*>(pdxldatum);
			}
	};
}



#endif // !GPDXL_CDXLDatumInt4_H

// EOF

