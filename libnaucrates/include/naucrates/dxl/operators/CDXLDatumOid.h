//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumOid.h
//
//	@doc:
//		Class for representing DXL oid datum
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumOid_H
#define GPDXL_CDXLDatumOid_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDatumOid
	//
	//	@doc:
	//		Class for representing DXL oid datums
	//
	//---------------------------------------------------------------------------
	class CDXLDatumOid : public CDXLDatum
	{
		private:
			// oid value
			OID m_oidVal;

			// private copy ctor
			CDXLDatumOid(const CDXLDatumOid &);

		public:
			// ctor
			CDXLDatumOid
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				BOOL fNull,
				OID oidVal
				);

			// dtor
			virtual
			~CDXLDatumOid(){};

			// accessor of oid value
			OID OidValue() const;

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser);

			// datum type
			virtual
			EdxldatumType Edxldt() const
			{
				return CDXLDatum::EdxldatumOid;
			}

			// is type passed by value
			virtual
			BOOL FByValue() const
			{
				return true;
			}

			// conversion function
			static
			CDXLDatumOid *PdxldatumConvert
				(
				CDXLDatum *pdxldatum
				)
			{
				GPOS_ASSERT(NULL != pdxldatum);
				GPOS_ASSERT(CDXLDatum::EdxldatumOid == pdxldatum->Edxldt());

				return dynamic_cast<CDXLDatumOid*>(pdxldatum);
			}
	};
}

#endif // !GPDXL_CDXLDatumOid_H

// EOF
