//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IDatumBool.h
//
//	@doc:
//		Base abstract class for bool representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumBool_H
#define GPNAUCRATES_IDatumBool_H

#include "gpos/base.h"
#include "naucrates/base/IDatumStatisticsMappable.h"

namespace gpnaucrates
{
	//---------------------------------------------------------------------------
	//	@class:
	//		IDatumBool
	//
	//	@doc:
	//		Base abstract class for bool representation
	//
	//---------------------------------------------------------------------------
	class IDatumBool : public IDatumStatisticsMappable
	{

		private:

			// private copy ctor
			IDatumBool(const IDatumBool &);

		public:

			// ctor
			IDatumBool()
			{};

			// dtor
			virtual
			~IDatumBool()
			{};

			// accessor for datum type
			virtual IMDType::ETypeInfo GetDatumType()
			{
				return IMDType::EtiBool;
			}

			// accessor of boolean value
			virtual
			BOOL GetValue() const = 0;

			// can datum be mapped to a double
			BOOL IsDatumMappableToDouble() const
			{
				return true;
			}

			// map to double for stats computation
			CDouble GetDoubleMapping() const
			{
				if (GetValue())
				{
					return CDouble(1.0);
				}
				
				return CDouble(0.0);
			}

			// can datum be mapped to LINT
			BOOL IsDatumMappableToLINT() const
			{
				return true;
			}

			// map to LINT for statistics computation
			LINT GetLINTMapping() const
			{
				if (GetValue())
				{
					return LINT(1);
				}
				return LINT(0);
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
				const IDatum * // datum
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
	}; // class IDatumBool

}


#endif // !GPNAUCRATES_IDatumBool_H

// EOF
