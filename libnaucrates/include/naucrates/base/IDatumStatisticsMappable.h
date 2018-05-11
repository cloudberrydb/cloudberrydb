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
			BOOL StatsMappable()
			{
				return this->StatsAreComparable(this);
			}

			// can datum be mapped to a double
			virtual
			BOOL IsDatumMappableToDouble() const = 0;

			// map to double for statistics computation
			virtual
			CDouble GetDoubleMapping() const = 0;

			// can datum be mapped to LINT
			virtual
			BOOL IsDatumMappableToLINT() const = 0;

			// map to LINT for statistics computation
			virtual
			LINT GetLINTMapping() const = 0;

			// statistics equality
			virtual
			BOOL StatsAreEqual(const IDatum *datum) const;

			// statistics less than
			virtual
			BOOL StatsAreLessThan(const IDatum *datum) const;

			// check if the given pair of datums are stats comparable
			virtual
			BOOL StatsAreComparable(const IDatum *datum) const;

			// distance function
			virtual
			CDouble GetStatsDistanceFrom(const IDatum *datum) const;

		// return double representation of mapping value
			CDouble GetValAsDouble() const;

	}; // class IDatumStatisticsMappable

}

#endif // !GPNAUCRATES_CDatumStatisticsMappable_H

// EOF
