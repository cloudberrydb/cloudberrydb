//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IDatumInt4.h
//
//	@doc:
//		Base abstract class for int4 representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumInt4_H
#define GPNAUCRATES_IDatumInt4_H

#include "gpos/base.h"
#include "naucrates/base/IDatumStatisticsMappable.h"

namespace gpnaucrates
{
	//---------------------------------------------------------------------------
	//	@class:
	//		IDatumInt4
	//
	//	@doc:
	//		Base abstract class for int4 representation
	//
	//---------------------------------------------------------------------------
	class IDatumInt4 : public IDatumStatisticsMappable
	{

		private:

			// private copy ctor
			IDatumInt4(const IDatumInt4 &);

		public:

			// ctor
			IDatumInt4()
			{};

			// dtor
			virtual
			~IDatumInt4()
			{};

			// accessor for datum type
			virtual  IMDType::ETypeInfo GetDatumType()
			{
				return IMDType::EtiInt4;
			}

			// accessor of integer value
			virtual
			INT Value() const = 0;

			// can datum be mapped to a double
			BOOL IsDatumMappableToDouble() const
			{
				return true;
			}

			// map to double for stats computation
			CDouble GetDoubleMapping() const
			{
				return CDouble(Value());
			}

			// can datum be mapped to LINT
			BOOL IsDatumMappableToLINT() const
			{
				return true;
			}

			// map to LINT for statistics computation
			LINT GetLINTMapping() const
			{
				return LINT(Value());
			}

			//  supports statistical comparisons based on the byte array representation of datum
			virtual
			BOOL SupportsBinaryComp
				(
				const IDatum * //datum
				) 
				const
			{
				return false;
			}

			// byte array representation of datum
			virtual
			const BYTE *GetByteArrayValue() const
			{
				GPOS_ASSERT(!"Invalid invocation of MakeCopyOfValue");
				return NULL;
			}

			// does the datum need to be padded before statistical derivation
			virtual
			BOOL NeedsPadding() const
			{
				return false;
			}

			// return the padded datum
			virtual
			IDatum *MakePaddedDatum
				(
				IMemoryPool *, // mp,
				ULONG    // col_len
				)
				const
			{
				GPOS_ASSERT(!"Invalid invocation of MakePaddedDatum");
				return NULL;
			}

			// statistics equality based on byte array representation of datums
			virtual
			BOOL StatsEqualBinary
				(
				const IDatum * //datum
				)
				const
			{
				GPOS_ASSERT(!"Invalid invocation of StatsEqualBinary");
				return false;
			}

			// statistics less than based on byte array representation of datums
			virtual
			BOOL StatsLessThanBinary
				(
				const IDatum * //datum
				)
				const
			{
				GPOS_ASSERT(!"Invalid invocation of StatsLessThanBinary");
				return false;
			}

			// does datum support like predicate
			virtual
			BOOL SupportsLikePredicate() const
			{
				return false;
			}

			// return the default scale factor of like predicate
			virtual
			CDouble GetLikePredicateScaleFactor() const
			{
				GPOS_ASSERT(!"Invalid invocation of DLikeSelectivity");
				return false;
			}

	}; // class IDatumInt4

}


#endif // !GPNAUCRATES_IDatumInt4_H

// EOF
