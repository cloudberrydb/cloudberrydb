//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		CDXLDatumStatsLintMappable.h
//
//	@doc:
//		Class for representing DXL datum of types having LINT mapping
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumStatsLintMappable_H
#define GPDXL_CDXLDatumStatsLintMappable_H

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
	//		CDXLDatumStatsLintMappable
	//
	//	@doc:
	//		Class for representing DXL datum of types having LINT mapping
	//
	//---------------------------------------------------------------------------
	class CDXLDatumStatsLintMappable : public CDXLDatumGeneric
	{
		private:

			// for statistics computation, map to LINT
			LINT m_lValue;

			// private copy ctor
			CDXLDatumStatsLintMappable(const CDXLDatumStatsLintMappable &);

		public:
			// ctor
			CDXLDatumStatsLintMappable
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iTypeModifier,
				BOOL fByVal,
				BOOL fNull,
				BYTE *pba,
				ULONG ulLength,
				LINT lValue
				);

			// dtor
			virtual
			~CDXLDatumStatsLintMappable(){};

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser);

			// datum type
			virtual
			EdxldatumType Edxldt() const
			{
				return CDXLDatum::EdxldatumStatsLintMappable;
			}

			// conversion function
			static
			CDXLDatumStatsLintMappable *PdxldatumConvert
				(
				CDXLDatum *pdxldatum
				)
			{
				GPOS_ASSERT(NULL != pdxldatum);
				GPOS_ASSERT(CDXLDatum::EdxldatumStatsLintMappable == pdxldatum->Edxldt());

				return dynamic_cast<CDXLDatumStatsLintMappable*>(pdxldatum);
			}

			// statistics related APIs

			// can datum be mapped to LINT
			virtual
			BOOL FHasStatsLINTMapping() const
			{
				return true;
			}

			// return the LINT mapping needed for statistics computation
			virtual
			LINT LStatsMapping() const
			{
				return m_lValue;
			}

	};
}

#endif // !GPDXL_CDXLDatumStatsLintMappable_H

// EOF
