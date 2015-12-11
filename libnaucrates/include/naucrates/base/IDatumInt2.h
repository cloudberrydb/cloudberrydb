//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		IDatumInt2.h
//
//	@doc:
//		Base abstract class for int2 representation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumInt2_H
#define GPNAUCRATES_IDatumInt2_H

#include "gpos/base.h"
#include "naucrates/base/IDatumStatisticsMappable.h"

namespace gpnaucrates
{
	//---------------------------------------------------------------------------
	//	@class:
	//		IDatumInt2
	//
	//	@doc:
	//		Base abstract class for int2 representation
	//
	//---------------------------------------------------------------------------
	class IDatumInt2 : public IDatumStatisticsMappable
	{

		private:

			// private copy ctor
			IDatumInt2(const IDatumInt2 &);

		public:

			// ctor
			IDatumInt2()
			{};

			// dtor
			virtual
			~IDatumInt2()
			{};

			// accessor for datum type
			virtual  IMDType::ETypeInfo Eti()
			{
				return IMDType::EtiInt2;
			}

			// accessor of integer value
			virtual
			SINT SValue() const = 0;

			// can datum be mapped to a double
			BOOL FHasStatsDoubleMapping() const
			{
				return true;
			}

			// map to double for stats computation
			CDouble DStatsMapping() const
			{
				return CDouble(SValue());
			}

			// can datum be mapped to LINT
			BOOL FHasStatsLINTMapping() const
			{
				return true;
			}

			// map to LINT for statistics computation
			LINT LStatsMapping() const
			{
				return LINT(SValue());
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

	}; // class IDatumInt2

}


#endif // !GPNAUCRATES_IDatumInt2_H

// EOF
