//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLColStats.h
//
//	@doc:
//		Class representing column stats
//---------------------------------------------------------------------------

#ifndef GPMD_CDXLColStats_H
#define GPMD_CDXLColStats_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDColStats.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CDXLBucket.h"

namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLColStats
	//
	//	@doc:
	//		Class representing column stats
	//
	//---------------------------------------------------------------------------
	class CDXLColStats : public IMDColStats
	{		
		private:
		
			// memory pool
			IMemoryPool *m_pmp;

			// metadata id of the object
			CMDIdColStats *m_pmdidColStats;
			
			// column name
			CMDName *m_pmdname;
			
			// column width
			CDouble m_dWidth;
			
			// null fraction
			CDouble m_dNullFreq;

			// ndistinct of remaining tuples
			CDouble m_dDistinctRemain;

			// frequency of remaining tuples
			CDouble m_dFreqRemain;

			// histogram buckets
			DrgPdxlbucket *m_pdrgpdxlbucket;
			
			// is column statistics missing in the database
			BOOL m_fColStatsMissing;

			// DXL string for object
			CWStringDynamic *m_pstr;
			
			// private copy ctor
			CDXLColStats(const CDXLColStats &);
		
		public:
			// ctor
			CDXLColStats
				(
				IMemoryPool *pmp,
				CMDIdColStats *pmdidColStats,
				CMDName *pmdname,
				CDouble dWidth,
				CDouble dNullFreq,
				CDouble dDistinctRemain,
				CDouble dFreqRemain,
				DrgPdxlbucket *pdrgpdxlbucket,
				BOOL fColStatsMissing
				);
			
			// dtor
			virtual
			~CDXLColStats();
			
			// the metadata id
			virtual 
			IMDId *Pmdid() const;
			
			// relation name
			virtual 
			CMDName Mdname() const;
			
			// DXL string representation of cache object 
			virtual 
			const CWStringDynamic *Pstr() const;
			
			// number of buckets
			virtual
			ULONG UlBuckets() const;

			// width
			virtual
			CDouble DWidth() const
			{
				return m_dWidth;
			}

			// null fraction
			virtual
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

			// is the column statistics missing in the database
			BOOL FColStatsMissing() const
			{
				return m_fColStatsMissing;
			}

			// get the bucket at the given position
			virtual
			const CDXLBucket *Pdxlbucket(ULONG ul) const;

			// serialize column stats in DXL format
			virtual 
			void Serialize(gpdxl::CXMLSerializer *) const;

#ifdef GPOS_DEBUG
			// debug print of the column stats
			virtual 
			void DebugPrint(IOstream &os) const;
#endif

			// dummy colstats
			static
			CDXLColStats *PdxlcolstatsDummy(IMemoryPool *pmp, IMDId *pmdid, CMDName *pmdname, CDouble dWidth);

	};

	// array of dxl column stats
	typedef CDynamicPtrArray<CDXLColStats, CleanupRelease> DrgPcolstats;
}



#endif // !GPMD_CDXLColStats_H

// EOF
