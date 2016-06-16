//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMDAggregate.h
//
//	@doc:
//		Interface for aggregates in the metadata cache
//---------------------------------------------------------------------------



#ifndef GPMD_IMDAggregate_H
#define GPMD_IMDAggregate_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"

namespace gpmd
{
	using namespace gpos;

	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDAggregate
	//
	//	@doc:
	//		Interface for aggregates in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDAggregate : public IMDCacheObject
	{		
		public:

			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtAgg;
			}
			
			// type of intermediate results computed by the aggregate's 
			// transformation function
			virtual 
			IMDId *PmdidTypeIntermediate() const = 0;

			// result type
			virtual 
			IMDId *PmdidTypeResult() const = 0;

			// is aggregate ordered
			virtual
			BOOL FOrdered() const = 0;
			
			// is aggregate splittable
			virtual
			BOOL FSplittable() const = 0;

			virtual
			// is aggregate hash capable
			BOOL FHashAggCapable() const = 0;
	};
}

#endif // !GPMD_IMDAggregate_H

// EOF
