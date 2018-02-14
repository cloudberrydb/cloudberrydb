//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumGenericGPDB.h
//
//	@doc:
//		GPDB-specific generic datum representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumGenericGPDB_H
#define GPNAUCRATES_CDatumGenericGPDB_H

#include "gpos/base.h"

#include "naucrates/base/IDatumGeneric.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"

#define GPDB_DATUM_HDRSZ 4

namespace gpnaucrates
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDatumGenericGPDB
	//
	//	@doc:
	//		GPDB-specific generic datum representation
	//
	//---------------------------------------------------------------------------
class CDatumGenericGPDB : public IDatumGeneric
{

	private:

		// memory pool
		IMemoryPool *m_pmp;

		// size in bytes
		ULONG m_ulSize;

		// a pointer to datum value
		BYTE *m_pbVal;

		// is null
		BOOL m_fNull;

		// type information
		IMDId *m_pmdid;

		INT m_iTypeModifier;

		// long int value used for statistic computation
		LINT m_lValue;

		// double value used for statistic computation
		CDouble m_dValue;

		// private copy ctor
		CDatumGenericGPDB(const CDatumGenericGPDB &);

	public:

		// ctor
		CDatumGenericGPDB
			(
			IMemoryPool *pmp,
			IMDId *pmdid,
			INT iTypeModifier,
			const void *pv,
			ULONG ulSize,
			BOOL fNull,
			LINT lValue,
			CDouble dValue
			);

		// dtor
		virtual
		~CDatumGenericGPDB();

		// accessor of metadata type id
		virtual
		IMDId *Pmdid() const;

		virtual
		INT ITypeModifier() const;

		// accessor of size
		virtual
		ULONG UlSize() const;

		// accessor of is null
		virtual
		BOOL FNull() const;

		// return string representation
		virtual
		const CWStringConst *Pstr(IMemoryPool *pmp) const;

		// hash function
		virtual
		ULONG UlHash() const;

		// match function for datums
		virtual
		BOOL FMatch(const IDatum *pdatum) const;

		// copy datum
		virtual
		IDatum *PdatumCopy(IMemoryPool *pmp) const;
		
		// print function
		virtual
		IOstream &OsPrint(IOstream &os) const;

		// accessor to bytearray, creates a copy
		virtual
		BYTE *PbaVal(IMemoryPool *pmp, ULONG *pulLength) const;

		// statistics related APIs

		// can datum be mapped to a double
		virtual
		BOOL FHasStatsDoubleMapping() const;

		// map to double for stats computation
		virtual
		CDouble DStatsMapping() const
		{
			GPOS_ASSERT(FHasStatsDoubleMapping());

			return m_dValue;
		}

		// can datum be mapped to LINT
		virtual
		BOOL FHasStatsLINTMapping() const;

		// map to LINT for statistics computation
		virtual
		LINT LStatsMapping() const
		{
			GPOS_ASSERT(FHasStatsLINTMapping());

			return m_lValue;
		}

		//  supports statistical comparisons based on the byte array representation of datum
		virtual
		BOOL FSupportsBinaryComp(const IDatum *pdatum) const;

		// byte array representation of datum
		virtual
		const BYTE *PbaVal() const;

		// stats equality
		virtual
		BOOL FStatsEqual(const IDatum *pdatum) const;

		// does the datum need to be padded before statistical derivation
		virtual
		BOOL FNeedsPadding() const;

		// return the padded datum
		virtual
		IDatum *PdatumPadded(IMemoryPool *pmp, ULONG ulColLen) const;

		// statistics equality based on byte array representation of datums
		virtual
		BOOL FStatsEqualBinary(const IDatum *pdatum) const;

		// statistics less than based on byte array representation of datums
		virtual
		BOOL FStatsLessThanBinary(const IDatum *pdatum) const;

		// does datum support like predicate
		virtual
		BOOL FSupportLikePredicate() const
		{
			return true;
		}

		// return the default scale factor of like predicate
		virtual
		CDouble DLikePredicateScaleFactor() const;

		// default selectivity of the trailing wildcards
		virtual
		CDouble DTrailingWildcardSelectivity(const BYTE *pba, ULONG ulPos) const;

		// selectivities needed for LIKE predicate statistics evaluation
		static
		const CDouble DDefaultFixedCharSelectivity;
		static
		const CDouble DDefaultCharRangeSelectivity;
		static
		const CDouble DDefaultAnyCharSelectivity;
		static
		const CDouble DDefaultCdbRanchorSelectivity;
		static
		const CDouble DDefaultCdbRolloffSelectivity;

	}; // class CDatumGenericGPDB
}


#endif // !GPNAUCRATES_CDatumGenericGPDB_H

// EOF
