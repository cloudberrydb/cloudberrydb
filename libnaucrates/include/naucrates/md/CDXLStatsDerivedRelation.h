//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLStatsDerivedRelation.h
//
//	@doc:
//		Class representing DXL derived relation statistics
//---------------------------------------------------------------------------

#ifndef GPMD_CDXLStatsDerivedRelation_H
#define GPMD_CDXLStatsDerivedRelation_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/md/CDXLStatsDerivedColumn.h"

namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLStatsDerivedRelation
	//
	//	@doc:
	//		Class representing DXL derived relation statistics
	//
	//---------------------------------------------------------------------------
	class CDXLStatsDerivedRelation : public CRefCount
	{
		private:

			// number of rows
			CDouble m_dRows;

			// flag to indicate if input relation is empty
			BOOL m_fEmpty;

			// array of derived column statistics
			DrgPdxlstatsdercol *m_pdrgpdxlstatsdercol;

			// private copy ctor
			CDXLStatsDerivedRelation(const CDXLStatsDerivedRelation &);

		public:

			// ctor
			CDXLStatsDerivedRelation
				(
				CDouble dRows,
				BOOL fEmpty,
				DrgPdxlstatsdercol *pdrgpdxldercolstat
				);

			// dtor
			virtual
			~CDXLStatsDerivedRelation();

			// number of rows
			CDouble DRows() const
			{
				return m_dRows;
			}

			// is statistics on an empty input
			virtual
			BOOL FEmpty() const
			{
				return m_fEmpty;
			}

			// derived column statistics
			const DrgPdxlstatsdercol *Pdrgpdxlstatsdercol() const;

			// serialize bucket in DXL format
			void Serialize(gpdxl::CXMLSerializer *) const;

#ifdef GPOS_DEBUG
			// debug print of the bucket
			void DebugPrint(IOstream &os) const;
#endif

	};

	// array of dxl buckets
	typedef CDynamicPtrArray<CDXLStatsDerivedRelation, CleanupRelease> DrgPdxlstatsderrel;
}

#endif // !GPMD_CDXLStatsDerivedRelation_H

// EOF
