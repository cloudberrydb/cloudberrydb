//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumInt4GPDB.h
//
//	@doc:
//		GPDB-specific int4 representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumInt4GPDB_H
#define GPNAUCRATES_CDatumInt4GPDB_H

#include "gpos/base.h"

#include "naucrates/base/IDatumInt4.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"

namespace gpnaucrates
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDatumInt4GPDB
	//
	//	@doc:
	//		GPDB-specific int4 representation
	//
	//---------------------------------------------------------------------------
class CDatumInt4GPDB : public IDatumInt4
{

	private:

		// type information
		IMDId *m_pmdid;
	
		// integer value
		INT m_iVal;

		// is null
		BOOL m_fNull;

		// private copy ctor
		CDatumInt4GPDB(const CDatumInt4GPDB &);
		
	public:

		// ctors
		CDatumInt4GPDB(CSystemId sysid, INT iVal, BOOL fNull = false);
		CDatumInt4GPDB(IMDId *pmdid, INT iVal, BOOL fNull = false);

		// dtor
		virtual
		~CDatumInt4GPDB();

		// accessor of metadata type id
		virtual
		IMDId *Pmdid() const;

		// accessor of size
		virtual
		ULONG UlSize() const;

		// accessor of integer value
		virtual
		INT IValue() const;

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

	}; // class CDatumInt4GPDB

}


#endif // !GPNAUCRATES_CDatumInt4GPDB_H

// EOF
