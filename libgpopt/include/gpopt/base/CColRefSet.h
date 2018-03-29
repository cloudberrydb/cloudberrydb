//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColRefSet.h
//
//	@doc:
//		Implementation of column reference sets based on bitset
//---------------------------------------------------------------------------
#ifndef GPOS_CColRefSet_H
#define GPOS_CColRefSet_H

#include "gpos/base.h"

#include "gpos/common/CBitSet.h"
#include "gpopt/base/CColRef.h"


#define GPOPT_COLREFSET_SIZE 1024

namespace gpopt
{

	// fwd decl
	class CColRefSet;
	
	// short hand for colref set array
	typedef CDynamicPtrArray<CColRefSet, CleanupRelease> DrgPcrs;

	// hash map mapping CColRef -> CColRefSet
	typedef CHashMap<CColRef, CColRefSet, gpos::UlHash<CColRef>, gpos::FEqual<CColRef>,
					CleanupNULL<CColRef>, CleanupRelease<CColRefSet> > HMCrCrs;

	// hash map mapping INT -> CColRef
	typedef CHashMap<INT, CColRef, gpos::UlHash<INT>, gpos::FEqual<INT>,
					CleanupDelete<INT>, CleanupNULL<CColRef> > HMICr;

	//---------------------------------------------------------------------------
	//	@class:
	//		CColRefSet
	//
	//	@doc:
	//		Column reference sets based on bitsets
	//
	//		Redefine accessors by bit index to be private to make super class' 
	//		member functions inaccessible
	//
	//---------------------------------------------------------------------------
	class CColRefSet : public CBitSet
	{
		// bitset iter needs to access internals
		friend class CColRefSetIter;
			
		private:
						
			// determine if bit is set
			BOOL FBit(ULONG ulBit) const;
			
			// set given bit; return previous value
			BOOL FExchangeSet(ULONG ulBit);
						
			// clear given bit; return previous value
			BOOL FExchangeClear(ULONG ulBit);

		public:
				
			// ctor
			explicit
			CColRefSet(IMemoryPool *pmp, ULONG ulSizeBits = GPOPT_COLREFSET_SIZE);

			explicit
			CColRefSet(IMemoryPool *pmp, const CColRefSet &);
			
			// ctor, copy from col refs array
			CColRefSet(IMemoryPool *pmp, const DrgPcr *pdrgpcr, ULONG ulSizeBits = GPOPT_COLREFSET_SIZE);

			// dtor
			~CColRefSet();
			
			// determine if bit is set
			BOOL FMember(const CColRef *pcr) const;
			
			// return random member
			CColRef *PcrAny() const;
			
			// return first member
			CColRef *PcrFirst() const;

			// include column
			void Include(const CColRef *pcr);

			// include column array
			void Include(const DrgPcr *pdrgpcr);

			// include column set
			void Include(const CColRefSet *pcrs);
						
			// remove column
			void Exclude(const CColRef *pcr);
			
			// remove column array
			void Exclude(const DrgPcr *pdrgpcr);

			// remove column set
			void Exclude(const CColRefSet *pcrs);
			
			// replace column with another column
			void Replace(const CColRef *pcrOut, const CColRef *pcrIn);

			// replace column array with another column array
			void Replace(const DrgPcr *pdrgpcrOut, const DrgPcr *pdrgpcrIn);

			// check if the current colrefset is a subset of any of the colrefsets
			// in the given array
			BOOL FContained(const DrgPcrs *pdrgpcrs);

			// convert to array
			DrgPcr *Pdrgpcr(IMemoryPool *pmp) const;

			// hash function
			ULONG UlHash();	

			// debug print
			IOstream &
			OsPrint(IOstream &os, ULONG ulLenMax = gpos::ulong_max) const;

			// extract all column ids
			void ExtractColIds(IMemoryPool *pmp, DrgPul *pdrgpulColIds) const;

			// are the columns in the column reference set covered by the array of column ref sets
			static
			BOOL FCovered(DrgPcrs *pdrgpcrs, CColRefSet *pcrs);

	}; // class CColRefSet


	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CColRefSet &crf)
	{
		return crf.OsPrint(os);
	}

}

#endif // !GPOS_CColRefSet_H


// EOF
