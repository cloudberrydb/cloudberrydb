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
	typedef CDynamicPtrArray<CColRef, CleanupNULL> CColRefArray;
	typedef CDynamicPtrArray<CColRefArray, CleanupRelease> CColRef2dArray;
	
	// hash map mapping ULONG -> CColRef
	typedef CHashMap<ULONG, CColRef, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
					CleanupDelete<ULONG>, CleanupNULL<CColRef> > UlongToColRefMap;
	// iterator
	typedef CHashMapIter<ULONG, CColRef, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
					CleanupDelete<ULONG>, CleanupNULL<CColRef> > UlongToColRefMapIter;
	
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
			const INT m_type_modifier;

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
			CColRef(const IMDType *pmdtype, const INT type_modifier, ULONG id, const CName *pname);

			// dtor
			virtual
			~CColRef();
			
			// accessor to type info
			const IMDType *RetrieveType() const
			{
				return m_pmdtype;
			}

			// type modifier
			INT TypeModifier() const
			{
				return m_type_modifier;
			}
			
			// name
			const CName &Name() const
			{
				return *m_pname;
			}

			// id
			ULONG Id() const
			{
				return m_id;
			}
			
			// overloaded equality operator
			BOOL operator ==
				(
				const CColRef &cr
				)
				const
			{
				return Equals(m_id, cr.Id());
			}

			// static hash functions
			static
			ULONG HashValue(const ULONG &);

			static
			ULONG HashValue(const CColRef *colref);

			 // equality function for hash table
			static
			BOOL Equals
				(
				const ULONG &ulKey,
				const ULONG &ulKeyOther
				)
			{
				return ulKey == ulKeyOther;
			}

			 // equality function
			static
			BOOL Equals
				(
				const CColRef *pcrFirst,
				const CColRef *pcrSecond
				)
			{
				return Equals(pcrFirst->Id(), pcrSecond->Id());
			}

			// extract array of colids from array of colrefs
			static
			ULongPtrArray *Pdrgpul(IMemoryPool *mp, CColRefArray *colref_array);

			// check if the the array of column references are equal
			static
			BOOL Equals(const CColRefArray *pdrgpcr1, const CColRefArray *pdrgpcr2);

			// check if the the array of column reference arrays are equal
			static
			BOOL Equals(const CColRef2dArray *pdrgdrgpcr1, const CColRef2dArray *pdrgdrgpcr2);

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
			const ULONG m_id;
			
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
