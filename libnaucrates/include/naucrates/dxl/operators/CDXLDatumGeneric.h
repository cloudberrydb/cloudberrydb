//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumGeneric.h
//
//	@doc:
//		Class for representing DXL datum of type generic
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumGeneric_H
#define GPDXL_CDXLDatumGeneric_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDatumGeneric
	//
	//	@doc:
	//		Class for representing DXL datum of type generic
	//
	//---------------------------------------------------------------------------
	class CDXLDatumGeneric: public CDXLDatum
	{
		private:

			// private copy ctor
			CDXLDatumGeneric(const CDXLDatumGeneric &);

		protected:

			// is datum passed by value or by reference
			BOOL m_fByVal;

			// datum byte array
			BYTE *m_pba;

		public:
			// ctor
			CDXLDatumGeneric
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iTypeModifier,
				BOOL fByVal,
				BOOL fNull,
				BYTE *pba,
				ULONG ulLength
				);

			// dtor
			virtual
			~CDXLDatumGeneric();

			// byte array
			const BYTE *Pba() const;

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser);

			// is type passed by value
			virtual BOOL FByValue() const
			{
				return m_fByVal;
			}

			// datum type
			virtual
			EdxldatumType Edxldt() const
			{
				return CDXLDatum::EdxldatumGeneric;
			}

			// conversion function
			static
			CDXLDatumGeneric *PdxldatumConvert
				(
				CDXLDatum *pdxldatum
				)
			{
				GPOS_ASSERT(NULL != pdxldatum);
				GPOS_ASSERT(CDXLDatum::EdxldatumGeneric == pdxldatum->Edxldt()
						|| CDXLDatum::EdxldatumStatsDoubleMappable == pdxldatum->Edxldt()
						|| CDXLDatum::EdxldatumStatsLintMappable == pdxldatum->Edxldt());

				return dynamic_cast<CDXLDatumGeneric*>(pdxldatum);
			}

			// statistics related APIs

			// can datum be mapped to LINT
			virtual
			BOOL FHasStatsLINTMapping() const
			{
				return false;
			}

			// return the lint mapping needed for statistics computation
			virtual
			LINT LStatsMapping() const
			{
				GPOS_ASSERT(FHasStatsLINTMapping());

				return 0;
			}

			// can datum be mapped to a double
			virtual
			BOOL FHasStatsDoubleMapping() const
			{
				return false;
			}

			// return the double mapping needed for statistics computation
			virtual
			CDouble DStatsMapping() const
			{
				GPOS_ASSERT(FHasStatsDoubleMapping());
				return 0;
			}
	};
}

#endif // !GPDXL_CDXLDatumGeneric_H

// EOF
