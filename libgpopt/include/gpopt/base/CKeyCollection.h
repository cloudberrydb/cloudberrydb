//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC CORP.
//
//	@filename:
//		CKeyCollection.h
//
//	@doc:
//		Encodes key sets for a relation
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CKeyCollection_H
#define GPOPT_CKeyCollection_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "gpopt/base/CColRefSet.h"


namespace gpopt
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CKeyCollection
	//
	//	@doc:
	//		Captures sets of keys for a relation
	//
	//---------------------------------------------------------------------------
	class CKeyCollection : public CRefCount
	{

		private:
		
			// memory pool
			IMemoryPool *m_pmp;
		
			// array of key sets
			DrgPcrs *m_pdrgpcrs;
			
			// private copy ctor
			CKeyCollection(const CKeyCollection &);

		public:

			// ctors
			explicit
			CKeyCollection(IMemoryPool *pmp);
			CKeyCollection(IMemoryPool *pmp, CColRefSet *pcrs);
			CKeyCollection(IMemoryPool *pmp, DrgPcr *pdrgpcr);

			// dtor
			virtual 
			~CKeyCollection();
			
			// add individual set -- takes ownership
			void Add(CColRefSet *pcrs);
			
			// check if set forms a key
			BOOL FKey(const CColRefSet *pcrs, BOOL fExactMatch = true) const;

			// check if an array of columns constitutes a key
			BOOL FKey(IMemoryPool *pmp, const DrgPcr *pdrgpcr) const;
			
			// trim off non-key columns
			DrgPcr *PdrgpcrTrim(IMemoryPool *pmp, const DrgPcr *pdrgpcr) const;
			
			// extract a key
			DrgPcr *PdrgpcrKey(IMemoryPool *pmp) const;

			// extract a hashable key
			DrgPcr *PdrgpcrHashableKey(IMemoryPool *pmp) const;

			// extract key at given position
			DrgPcr *PdrgpcrKey(IMemoryPool *pmp, ULONG ul) const;

			// extract key at given position
			CColRefSet *PcrsKey(IMemoryPool *pmp, ULONG ul) const;
			
			// number of keys
			ULONG UlKeys() const
			{
				return m_pdrgpcrs->UlLength();
			}

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const;

	}; // class CKeyCollection

 	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CKeyCollection &kc)
	{
		return kc.OsPrint(os);
	}

}

#endif // !GPOPT_CKeyCollection_H

// EOF
