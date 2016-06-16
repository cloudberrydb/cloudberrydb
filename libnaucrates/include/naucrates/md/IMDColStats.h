//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDColStats.h
//
//	@doc:
//		Interface for column stats
//---------------------------------------------------------------------------



#ifndef GPMD_IMDColStats_H
#define GPMD_IMDColStats_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/CDXLBucket.h"

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	//---------------------------------------------------------------------------
	//	@class:
	//		IMDColStats
	//
	//	@doc:
	//		Interface for column stats
	//
	//---------------------------------------------------------------------------
	class IMDColStats : public IMDCacheObject
	{		
		public:
			
			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtColStats;
			}
		
			// number of buckets
			virtual
			ULONG UlBuckets() const = 0;
			
			// width
			virtual
			CDouble DWidth() const = 0;

			// null fraction
			virtual
			CDouble DNullFreq() const = 0;

			// ndistinct of remaining tuples
			virtual
			CDouble DDistinctRemain() const = 0;

			// frequency of remaining tuples
			virtual
			CDouble DFreqRemain() const = 0;

			// is the columns statistics missing in the database
			virtual
			BOOL FColStatsMissing() const = 0;

			// get the bucket at the given position
			virtual
			const CDXLBucket *Pdxlbucket(ULONG ul) const = 0;
	};
}


#endif // !GPMD_IMDColStats_H

// EOF
