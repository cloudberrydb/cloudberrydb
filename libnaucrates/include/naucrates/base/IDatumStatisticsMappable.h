//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IDatumStatisticsMappable.h
//
//	@doc:
//		Abstract class that implements statistics operations
//		based on mapping to LINT or CDouble
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumStatisticsMappable_H
#define GPNAUCRATES_CDatumStatisticsMappable_H

#include "gpos/base.h"

#include "naucrates/base/IDatum.h"

namespace gpnaucrates
{
	//---------------------------------------------------------------------------
	//	@class:
	//		IDatumStatisticsMappable
	//
	//	@doc:
	//		Abstract class that implements statistics operations
	//		based on mapping to LINT
	//
	//---------------------------------------------------------------------------
	class IDatumStatisticsMappable : public IDatum
	{
		private:

			// private copy ctor
			IDatumStatisticsMappable(const IDatumStatisticsMappable &);

		public:

			// ctor
			IDatumStatisticsMappable()
			{};

			// dtor
			virtual
			~IDatumStatisticsMappable()
			{};
			
			// is datum mappable to a base type for statistics purposes
			virtual 
			BOOL FStatsMappable()
			{
				return this->FStatsComparable(this);
			}

			// can datum be mapped to a double
			virtual
			BOOL FHasStatsDoubleMapping() const = 0;

			// map to double for statistics computation
			virtual
			CDouble DStatsMapping() const = 0;

			// can datum be mapped to LINT
			virtual
			BOOL FHasStatsLINTMapping() const = 0;

			// map to LINT for statistics computation
			virtual
			LINT LStatsMapping() const = 0;

			// statistics equality
			virtual
			BOOL FStatsEqual(const IDatum *pdatum) const;

			// statistics less than
			virtual
			BOOL FStatsLessThan(const IDatum *pdatum) const;

			// check if the given pair of datums are stats comparable
			virtual
			BOOL FStatsComparable(const IDatum *pdatum) const;

			// distance function
			virtual
			CDouble DStatsDistance(const IDatum *pdatum) const;

			// return double representation of mapping value
			CDouble DMappingVal() const;

	}; // class IDatumStatisticsMappable

}

#endif // !GPNAUCRATES_CDatumStatisticsMappable_H

// EOF
