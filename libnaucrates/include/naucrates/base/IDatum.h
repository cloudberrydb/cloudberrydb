//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IDatum.h
//
//	@doc:
//		Base abstract class for datum representation inside optimizer
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatum_H
#define GPNAUCRATES_IDatum_H

#include "gpos/base.h"
#include "gpos/string/CWStringConst.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CHashMap.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;

	class IDatum;

	// hash map mapping ULONG -> Datum
	typedef CHashMap<ULONG, IDatum, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<IDatum> > UlongToIDatumMap;

	//---------------------------------------------------------------------------
	//	@class:
	//		IDatum
	//
	//	@doc:
	//		Base abstract class for datum representation inside optimizer
	//
	//---------------------------------------------------------------------------
	class IDatum: public CRefCount
	{
		private:

			// private copy ctor
			IDatum(const IDatum &);

		public:

			// ctor
			IDatum()
			{};

			// dtor
			virtual
			~IDatum()
			{};

			// accessor for datum type
			virtual IMDType::ETypeInfo GetDatumType() = 0;

			// accessor of metadata id
			virtual
			IMDId *MDId() const = 0;

			virtual
			INT TypeModifier() const
			{
				return default_type_modifier;
			}

			// accessor of size
			virtual
			ULONG Size() const = 0;

			// is datum null?
			virtual
			BOOL IsNull() const = 0;

			// return string representation
			virtual
			const CWStringConst *GetStrRepr(CMemoryPool *mp) const = 0;

			// hash function
			virtual
			ULONG HashValue() const = 0;

			// Match function on datums
			virtual
			BOOL Matches(const IDatum *) const = 0;
			
			// create a copy of the datum
			virtual
			IDatum *MakeCopy(CMemoryPool *mp) const = 0;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const = 0;

			// statistics related APIs

			// is datum mappable to a base type for stats purposes
			virtual 
			BOOL StatsMappable() 
			{
				// not mappable by default
				return false;
			}
			
			virtual
			BOOL StatsAreEqual(const IDatum *datum) const = 0;

			// stats less than
			virtual
			BOOL StatsAreLessThan(const IDatum *datum) const = 0;

			// check if the given pair of datums are stats comparable
			virtual
			BOOL StatsAreComparable(const IDatum *datum) const = 0;

			// stats greater than
			virtual
			BOOL StatsAreGreaterThan
					(
							const IDatum *datum
					)
				const
			{
				BOOL stats_are_comparable = datum->StatsAreComparable(this);
				GPOS_ASSERT(stats_are_comparable && "Invalid invocation of StatsAreGreaterThan");
				return stats_are_comparable && datum->StatsAreLessThan(this);
			}

			// distance function
			virtual
			CDouble GetStatsDistanceFrom(const IDatum *) const = 0;

			// does the datum need to be padded before statistical derivation
			virtual
			BOOL NeedsPadding() const = 0;

			// return the padded datum
			virtual
			IDatum *MakePaddedDatum(CMemoryPool *mp, ULONG col_len) const = 0;

			// does datum support like predicate
			virtual
			BOOL SupportsLikePredicate() const = 0;

			// return the default scale factor of like predicate
			virtual
			CDouble GetLikePredicateScaleFactor() const = 0;

			// byte array for char/varchar columns
			virtual
			const BYTE *GetByteArrayValue() const = 0;

			// comparison function sorting idatums
			static
			inline INT StatsCmp(const void *val1, const void *val2);

	}; // class IDatum

	// comparison function for statistics operations
	INT IDatum::StatsCmp
		(
		const void *val1,
		const void *val2
		)
	{
		const IDatum *datum1 = *(const IDatum **) (val1);
		const IDatum *datum2 = *(const IDatum **) (val2);

		if (datum1->StatsAreEqual(datum2))
		{
			return 0;
		}
		
		if (datum1->StatsAreComparable(datum2) && datum1->StatsAreLessThan(datum2))
		{
			return -1;
		}
		return 1;
	}

	// array of idatums
	typedef CDynamicPtrArray<IDatum, CleanupRelease> IDatumArray;
}  // namespace gpnaucrates


#endif // !GPNAUCRATES_IDatum_H

// EOF
