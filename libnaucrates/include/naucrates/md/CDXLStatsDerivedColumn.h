//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLStatsDerivedColumn.h
//
//	@doc:
//		Class representing DXL derived column statistics
//---------------------------------------------------------------------------

#ifndef GPMD_CDXLStatsDerivedColumn_H
#define GPMD_CDXLStatsDerivedColumn_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/md/CDXLBucket.h"

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
	//		CDXLStatsDerivedColumn
	//
	//	@doc:
	//		Class representing DXL derived column statistics
	//
	//---------------------------------------------------------------------------
	class CDXLStatsDerivedColumn : public CRefCount
	{
		private:

			// column identifier
			ULONG m_ulColId;

			// column width
			CDouble m_dWidth;

			// null fraction
			CDouble m_dNullFreq;

			// ndistinct of remaining tuples
			CDouble m_dDistinctRemain;

			// frequency of remaining tuples
			CDouble m_dFreqRemain;

			DrgPdxlbucket *m_pdrgpdxlbucket;

			// private copy ctor
			CDXLStatsDerivedColumn(const CDXLStatsDerivedColumn &);

		public:

			// ctor
			CDXLStatsDerivedColumn
				(
				ULONG ulColId,
				CDouble dWidth,
				CDouble dNullFreq,
				CDouble dDistinctRemain,
				CDouble dFreqRemain,
				DrgPdxlbucket *pdrgpdxlbucket
				);

			// dtor
			virtual
			~CDXLStatsDerivedColumn();

			// column identifier
			ULONG UlColId() const
			{
				return m_ulColId;
			}

			// column width
			CDouble DWidth() const
			{
				return m_dWidth;
			}

			// null fraction of this column
			CDouble DNullFreq() const
			{
				return m_dNullFreq;
			}

			// ndistinct of remaining tuples
			CDouble DDistinctRemain() const
			{
				return m_dDistinctRemain;
			}

			// frequency of remaining tuples
			CDouble DFreqRemain() const
			{
				return m_dFreqRemain;
			}

			const DrgPdxlbucket *Pdrgpdxlbucket() const;

			// serialize bucket in DXL format
			void Serialize(gpdxl::CXMLSerializer *) const;

#ifdef GPOS_DEBUG
			// debug print of the bucket
			void DebugPrint(IOstream &os) const;
#endif

	};

	// array of dxl buckets
	typedef CDynamicPtrArray<CDXLStatsDerivedColumn, CleanupRelease> DrgPdxlstatsdercol;
}

#endif // !GPMD_CDXLStatsDerivedColumn_H

// EOF
