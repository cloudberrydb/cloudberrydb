//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartKeys.h
//
//	@doc:
//		A collection of partitioning keys for a partitioned table
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartKeys_H
#define GPOPT_CPartKeys_H

#include "gpos/base.h"
#include "gpopt/base/CColRef.h"

namespace gpopt
{
	using namespace gpos;

	// fwd decl
	class CColRefSet;
	class CPartKeys;

	// array of part keys
	typedef CDynamicPtrArray<CPartKeys, CleanupRelease> DrgPpartkeys;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPartKeys
	//
	//	@doc:
	//		A collection of partitioning keys for a partitioned table
	//
	//---------------------------------------------------------------------------
	class CPartKeys : public CRefCount
	{
		private:

			// partitioning keys
			DrgDrgPcr *m_pdrgpdrgpcr;

			// number of levels
			ULONG m_ulLevels;

			// private copy ctor
			CPartKeys(const CPartKeys &);

		public:

			// ctor
			explicit
			CPartKeys(DrgDrgPcr *pdrgpdrgpcr);

			// dtor
			~CPartKeys();

			// return key at a given level
			CColRef *PcrKey(ULONG ulLevel) const;

			// return array of keys
			DrgDrgPcr *Pdrgpdrgpcr() const
			{
				return m_pdrgpdrgpcr;
			}

			// number of levels
			ULONG UlLevels() const
			{
				return m_ulLevels;
			}

			// copy part key into the given memory pool
			CPartKeys *PpartkeysCopy(IMemoryPool *pmp);

			// check whether the key columns overlap the given column
			BOOL FOverlap(CColRefSet *pcrs) const;

			// create a new PartKeys object from the current one by remapping the
			// keys using the given hashmap
			CPartKeys *PpartkeysRemap(IMemoryPool *pmp, HMUlCr *phmulcr) const;

			// print
			IOstream &OsPrint(IOstream &) const;

			// copy array of part keys into given memory pool
			static
			DrgPpartkeys *PdrgppartkeysCopy(IMemoryPool *pmp, const DrgPpartkeys *pdrgppartkeys);

#ifdef GPOS_DEBUG
			// debug print for interactive debugging sessions only
			void DbgPrint() const;
#endif // GPOS_DEBUG
	}; // CPartKeys

	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CPartKeys &partkeys)
	{
		return partkeys.OsPrint(os);
	}

}

#endif // !GPOPT_CPartKeys_H

// EOF

