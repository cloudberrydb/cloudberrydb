//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CEnumSetIter.h
//
//	@doc:
//		Implementation of iterator for enum set
//---------------------------------------------------------------------------
#ifndef GPOS_CEnumSetIter_H
#define GPOS_CEnumSetIter_H

#include "gpos/base.h"
#include "gpos/common/CEnumSet.h"
#include "gpos/common/CBitSetIter.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CEnumSetIter
	//
	//	@doc:
	//		Template derived from CBitSetIter
	//
	//---------------------------------------------------------------------------
	template<class T, ULONG eSentinel>
	class CEnumSetIter : public CBitSetIter
	{

		private:

			// private copy ctor
			CEnumSetIter<T, eSentinel>(const CEnumSetIter<T, eSentinel>&);
			
		public:
				
			// ctor
			explicit
			CEnumSetIter<T, eSentinel>(const CEnumSet<T, eSentinel> &es)
				:
				CBitSetIter(es)
			{}
			
			// dtor
			~CEnumSetIter<T, eSentinel>() {}

			// current enum
			T TBit() const
			{
				GPOS_ASSERT(eSentinel > CBitSetIter::UlBit() && "Out of range of enum");
				return static_cast<T>(CBitSetIter::UlBit());				
			}

	}; // class CEnumSetIter
}


#endif // !GPOS_CEnumSetIter_H

// EOF

