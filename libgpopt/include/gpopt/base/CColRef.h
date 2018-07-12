//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CColRef.h
//
//	@doc:
//		Column reference implementation
//---------------------------------------------------------------------------
#ifndef GPOS_CColRef_H
#define GPOS_CColRef_H

#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"

#include "gpopt/metadata/CName.h"

#include "naucrates/md/IMDType.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	class CColRef;

	// colref array
	typedef CDynamicPtrArray<CColRef, CleanupNULL> DrgPcr;
	typedef CDynamicPtrArray<DrgPcr, CleanupRelease> DrgDrgPcr;
	
	// hash map mapping ULONG -> CColRef
	typedef CHashMap<ULONG, CColRef, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupNULL<CColRef> > HMUlCr;
	// iterator
	typedef CHashMapIter<ULONG, CColRef, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupNULL<CColRef> > HMIterUlCr;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CColRef
	//
	//	@doc:
	//		Column Reference base class
	//		Non-refcounted objects; passed by reference; managed by separate
	//		factory object
	//
	//---------------------------------------------------------------------------
	class CColRef
	{
		private:
			
			// type information
			const IMDType *m_pmdtype;

			// type modifier
			const INT m_iTypeModifier;

			// name: SQL alias or artificial name
			const CName *m_pname;

			// private copy ctor
			CColRef(const CColRef &);
			
		public:
		
			enum Ecolreftype
			{
				EcrtTable,
				EcrtComputed,
				
				EcrtSentinel
			};
		
			// ctor
			CColRef(const IMDType *pmdtype, const INT iTypeModifier, ULONG ulId, const CName *pname);

			// dtor
			virtual
			~CColRef();
			
			// accessor to type info
			const IMDType *Pmdtype() const
			{
				return m_pmdtype;
			}

			// type modifier
			INT ITypeModifier() const
			{
				return m_iTypeModifier;
			}
			
			// name
			const CName &Name() const
			{
				return *m_pname;
			}

			// id
			ULONG UlId() const
			{
				return m_ulId;
			}
			
			// overloaded equality operator
			BOOL operator ==
				(
				const CColRef &cr
				)
				const
			{
				return FEqual(m_ulId, cr.UlId());
			}

			// static hash functions
			static
			ULONG UlHash(const ULONG &);

			static
			ULONG UlHash(const CColRef *pcr);

			 // equality function for hash table
			static
			BOOL FEqual
				(
				const ULONG &ulKey,
				const ULONG &ulKeyOther
				)
			{
				return ulKey == ulKeyOther;
			}

			 // equality function
			static
			BOOL FEqual
				(
				const CColRef *pcrFirst,
				const CColRef *pcrSecond
				)
			{
				return FEqual(pcrFirst->UlId(), pcrSecond->UlId());
			}

			// extract array of colids from array of colrefs
			static
			DrgPul *Pdrgpul(IMemoryPool *pmp, DrgPcr *pdrgpcr);

			// check if the the array of column references are equal
			static
			BOOL FEqual(const DrgPcr *pdrgpcr1, const DrgPcr *pdrgpcr2);

			// check if the the array of column reference arrays are equal
			static
			BOOL FEqual(const DrgDrgPcr *pdrgdrgpcr1, const DrgDrgPcr *pdrgdrgpcr2);

			// type of column reference (base/computed)
			virtual
			Ecolreftype Ecrt() const = 0;

			// is column a system column?
			virtual
			BOOL FSystemCol() const = 0;

			// print
			IOstream &OsPrint(IOstream &) const;

			// link for hash chain
			SLink m_link;

			// id, serves as hash key
			const ULONG m_ulId;
			
			// invalid key
			static
			const ULONG m_ulInvalid;

#ifdef GPOS_DEBUG
			void DbgPrint() const;
#endif  // GPOS_DEBUG

	}; // class CColRef

 	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CColRef &cr)
	{
		return cr.OsPrint(os);
	}
	
}

#endif // !GPOS_CColRef_H

// EOF
