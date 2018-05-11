//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IDatumInt8.h
//
//	@doc:
//		Base abstract class for int8 representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumInt8_H
#define GPNAUCRATES_IDatumInt8_H

#include "gpos/base.h"
#include "naucrates/base/IDatumStatisticsMappable.h"

namespace gpnaucrates
{
	//---------------------------------------------------------------------------
	//	@class:
	//		IDatumInt8
	//
	//	@doc:
	//		Base abstract class for int8 representation
	//
	//---------------------------------------------------------------------------
	class IDatumInt8 : public IDatumStatisticsMappable
	{

		private:

			// private copy ctor
			IDatumInt8(const IDatumInt8 &);

		public:

			// ctor
			IDatumInt8()
			{}

			// dtor
			virtual
			~IDatumInt8()
			{}

			// accessor for datum type
			virtual  IMDType::ETypeInfo GetDatumType()
			{
				return IMDType::EtiInt8;
			}

			// accessor of integer value
			virtual
			LINT Value() const = 0;

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
				return Value();
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
	}; // class IDatumInt8

}


#endif // !GPNAUCRATES_IDatumInt8_H

// EOF
