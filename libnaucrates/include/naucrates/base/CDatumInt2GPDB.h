//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDatumInt2GPDB.h
//
//	@doc:
//		GPDB-specific int2 representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumInt2GPDB_H
#define GPNAUCRATES_CDatumInt2GPDB_H

#include "gpos/base.h"

#include "naucrates/base/IDatumInt2.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"

namespace gpnaucrates
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDatumInt2GPDB
	//
	//	@doc:
	//		GPDB-specific int2 representation
	//
	//---------------------------------------------------------------------------
class CDatumInt2GPDB : public IDatumInt2
{

	private:

		// type information
		IMDId *m_pmdid;
	
		// integer value
		SINT m_sVal;

		// is null
		BOOL m_fNull;

		// private copy ctor
		CDatumInt2GPDB(const CDatumInt2GPDB &);
		
	public:

		// ctors
		CDatumInt2GPDB(CSystemId sysid, SINT sVal, BOOL fNull = false);
		CDatumInt2GPDB(IMDId *pmdid, SINT sVal, BOOL fNull = false);

		// dtor
		virtual
		~CDatumInt2GPDB();

		// accessor of metadata type id
		virtual
		IMDId *Pmdid() const;

		// accessor of size
		virtual
		ULONG UlSize() const;

		// accessor of integer value
		virtual
		SINT SValue() const;

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
		BOOL FMatch(const IDatum *) const;

		// copy datum
		virtual
		IDatum *PdatumCopy(IMemoryPool *pmp) const;
		
		// print function
		virtual
		IOstream &OsPrint(IOstream &os) const;

	}; // class CDatumInt2GPDB

}


#endif // !GPNAUCRATES_CDatumInt2GPDB_H

// EOF
