//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLDatumStatsDoubleMappable.h
//
//	@doc:
//		Class for representing DXL datum of types having double mapping
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumStatsDoubleMappable_H
#define GPDXL_CDXLDatumStatsDoubleMappable_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDatumStatsDoubleMappable
	//
	//	@doc:
	//		Class for representing DXL datum of types having double mapping
	//
	//---------------------------------------------------------------------------
	class CDXLDatumStatsDoubleMappable: public CDXLDatumGeneric
	{
		private:

			// for statistics computation, map to double
			CDouble m_dValue;

			// private copy ctor
			CDXLDatumStatsDoubleMappable(const CDXLDatumStatsDoubleMappable &);

		public:
			// ctor
			CDXLDatumStatsDoubleMappable
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iTypeModifier,
				BOOL fByVal,
				BOOL fNull,
				BYTE *pba,
				ULONG ulLength,
				CDouble dValue
				);

			// dtor
			virtual
			~CDXLDatumStatsDoubleMappable(){};

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser);

			// datum type
			virtual
			EdxldatumType Edxldt() const
			{
				return CDXLDatum::EdxldatumStatsDoubleMappable;
			}

			// statistics related APIs

			// can datum be mapped to double
			virtual
			BOOL FHasStatsDoubleMapping() const
			{
				return true;
			}

			// return the double mapping needed for statistics computation
			virtual
			CDouble DStatsMapping() const
			{
				return m_dValue;
			}

			// conversion function
			static
			CDXLDatumStatsDoubleMappable *PdxldatumConvert
				(
				CDXLDatum *pdxldatum
				)
			{
				GPOS_ASSERT(NULL != pdxldatum);
				GPOS_ASSERT(CDXLDatum::EdxldatumStatsDoubleMappable == pdxldatum->Edxldt());

				return dynamic_cast<CDXLDatumStatsDoubleMappable*>(pdxldatum);
			}
	};
}

#endif // !GPDXL_CDXLDatumStatsDoubleMappable_H

// EOF
