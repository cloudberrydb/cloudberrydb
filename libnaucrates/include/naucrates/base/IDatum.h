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
	typedef CHashMap<ULONG, IDatum, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<IDatum> > HMUlDatum;

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
			virtual IMDType::ETypeInfo Eti() = 0;

			// accessor of metadata id
			virtual
			IMDId *Pmdid() const = 0;

			virtual
			INT ITypeModifier() const
			{
				return IDefaultTypeModifier;
			}

			// accessor of size
			virtual
			ULONG UlSize() const = 0;

			// is datum null?
			virtual
			BOOL FNull() const = 0;

			// return string representation
			virtual
			const CWStringConst *Pstr(IMemoryPool *pmp) const = 0;

			// hash function
			virtual
			ULONG UlHash() const = 0;

			// Match function on datums
			virtual
			BOOL FMatch(const IDatum *) const = 0;
			
			// create a copy of the datum
			virtual
			IDatum *PdatumCopy(IMemoryPool *pmp) const = 0;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const = 0;

			// statistics related APIs

			// is datum mappable to a base type for stats purposes
			virtual 
			BOOL FStatsMappable() 
			{
				// not mappable by default
				return false;
			}
			
			virtual
			BOOL FStatsEqual(const IDatum *pdatum) const = 0;

			// stats less than
			virtual
			BOOL FStatsLessThan(const IDatum *pdatum) const = 0;

			// check if the given pair of datums are stats comparable
			virtual
			BOOL FStatsComparable(const IDatum *pdatum) const = 0;

			// stats greater than
			virtual
			BOOL FStatsGreaterThan
				(
				const IDatum *pdatum
				)
				const
			{
				BOOL fStatsComparable = pdatum->FStatsComparable(this);
				GPOS_ASSERT(fStatsComparable && "Invalid invocation of FStatsGreaterThan");
				return fStatsComparable && pdatum->FStatsLessThan(this);
			}

			// distance function
			virtual
			CDouble DStatsDistance(const IDatum *) const = 0;

			// does the datum need to be padded before statistical derivation
			virtual
			BOOL FNeedsPadding() const = 0;

			// return the padded datum
			virtual
			IDatum *PdatumPadded(IMemoryPool *pmp, ULONG ulColLen) const = 0;

			// does datum support like predicate
			virtual
			BOOL FSupportLikePredicate() const = 0;

			// return the default scale factor of like predicate
			virtual
			CDouble DLikePredicateScaleFactor() const = 0;

			// supports statistical comparisons based on the byte array representation of datum
			virtual
			BOOL FSupportsBinaryComp(const IDatum *pdatum) const = 0;

			// byte array for char/varchar columns
			virtual
			const BYTE *PbaVal() const = 0;

			// statistics equality based on byte array representation of datums
			virtual
			BOOL FStatsEqualBinary(const IDatum *pdatum) const = 0;

			// statistics less than based on byte array representation of datums
			virtual
			BOOL FStatsLessThanBinary(const IDatum *pdatum) const = 0;

			// comparison function sorting idatums
			static
			inline INT IStatsCmp(const void *pv1, const void *pv2);

	}; // class IDatum

	// comparison function for statistics operations
	INT IDatum::IStatsCmp
		(
		const void *pv1,
		const void *pv2
		)
	{
		const IDatum *pdatum1 = *(const IDatum **) (pv1);
		const IDatum *pdatum2 = *(const IDatum **) (pv2);

		if (pdatum1->FStatsEqual(pdatum2))
		{
			return 0;
		}
		
		if (pdatum1->FStatsComparable(pdatum2) && pdatum1->FStatsLessThan(pdatum2))
		{
			return -1;
		}
		return 1;
	}

	// array of idatums
	typedef CDynamicPtrArray<IDatum, CleanupRelease> DrgPdatum;
}


#endif // !GPNAUCRATES_IDatum_H

// EOF
