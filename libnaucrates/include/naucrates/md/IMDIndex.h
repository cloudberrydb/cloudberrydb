//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDIndex.h
//
//	@doc:
//		Interface for indexes in the metadata cache
//---------------------------------------------------------------------------



#ifndef GPMD_IMDIndex_H
#define GPMD_IMDIndex_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"

namespace gpmd
{
	using namespace gpos;

	// fwd decl
	class IMDPartConstraint;
	class IMDScalarOp;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDIndex
	//
	//	@doc:
	//		Interface for indexes in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDIndex : public IMDCacheObject
	{
		public:

			// index type
			enum EmdindexType
			{
				EmdindBtree,	// btree
				EmdindBitmap,	// bitmap
				EmdindSentinel
			};

			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtInd;
			}

			// is the index clustered
			virtual
			BOOL FClustered() const = 0;
			
			// index type
			virtual
			EmdindexType Emdindt() const = 0;
			
			// number of keys
			virtual
			ULONG UlKeys() const = 0;
			
			// return the n-th key column
			virtual
			ULONG UlKey(ULONG ulPos) const = 0;

			// return the position of the key column
			virtual
			ULONG UlPosInKey(ULONG ulPos) const = 0;

			// number of included columns
			virtual
			ULONG UlIncludedCols() const = 0;

			// return the n-th included column
			virtual
			ULONG UlIncludedCol(ULONG ulPos) const = 0;

			// return the position of the included column
			virtual
			ULONG UlPosInIncludedCol(ULONG ulCol) const = 0;
			
			// part constraint
			virtual
			IMDPartConstraint *Pmdpartcnstr() const = 0;
			
			// type id of items returned by the index
			virtual
			IMDId *PmdidItemType() const = 0;

			// check if given scalar comparison can be used with the index key 
			// at the specified position
			virtual 
			BOOL FCompatible(const IMDScalarOp *pmdscop, ULONG ulKeyPos) const = 0;
			
			// index type as a string value
			static
			const CWStringConst *PstrIndexType(EmdindexType emdindt);
	};
}



#endif // !GPMD_IMDIndex_H

// EOF
