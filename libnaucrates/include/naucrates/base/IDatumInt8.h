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
			virtual  IMDType::ETypeInfo Eti()
			{
				return IMDType::EtiInt8;
			}

			// accessor of integer value
			virtual
			LINT LValue() const = 0;

			// can datum be mapped to a double
			BOOL FHasStatsDoubleMapping() const
			{
				return true;
			}

			// map to double for stats computation
			CDouble DStatsMapping() const
			{
				return CDouble(LValue());
			}

			// can datum be mapped to LINT
			BOOL FHasStatsLINTMapping() const
			{
				return true;
			}

			// map to LINT for statistics computation
			LINT LStatsMapping() const
			{
				return LValue();
			}

			//  supports statistical comparisons based on the byte array representation of datum
			virtual
			BOOL FSupportsBinaryComp
				(
				const IDatum * //pdatum
				)
				const
			{
				return false;
			}

			// byte array representation of datum
			virtual
			const BYTE *PbaVal() const
			{
				GPOS_ASSERT(!"Invalid invocation of PbaVal");
				return NULL;
			}

			// does the datum need to be padded before statistical derivation
			virtual
			BOOL FNeedsPadding() const
			{
				return false;
			}

			// return the padded datum
			virtual
			IDatum *PdatumPadded
				(
				IMemoryPool *, // pmp,
				ULONG    // ulColLen
				)
				const
			{
				GPOS_ASSERT(!"Invalid invocation of PdatumPadded");
				return NULL;
			}

			// statistics equality based on byte array representation of datums
			virtual
			BOOL FStatsEqualBinary
				(
				const IDatum * //pdatum
				)
				const
			{
				GPOS_ASSERT(!"Invalid invocation of FStatsEqualBinary");
				return false;
			}

			// statistics less than based on byte array representation of datums
			virtual
			BOOL FStatsLessThanBinary
				(
				const IDatum * //pdatum
				)
				const
			{
				GPOS_ASSERT(!"Invalid invocation of FStatsLessThanBinary");
				return false;
			}

			// does datum support like predicate
			virtual
			BOOL FSupportLikePredicate() const
			{
				return false;
			}

			// return the default scale factor of like predicate
			virtual
			CDouble DLikePredicateScaleFactor() const
			{
				GPOS_ASSERT(!"Invalid invocation of DLikeSelectivity");
				return false;
			}
	}; // class IDatumInt8

}


#endif // !GPNAUCRATES_IDatumInt8_H

// EOF
