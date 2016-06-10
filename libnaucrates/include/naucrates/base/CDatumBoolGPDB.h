//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumBoolGPDB.h
//
//	@doc:
//		GPDB-specific bool representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumBoolGPDB_H
#define GPNAUCRATES_CDatumBoolGPDB_H

#include "gpos/base.h"
#include "naucrates/base/IDatumBool.h"

#include "naucrates/md/CMDTypeBoolGPDB.h"

namespace gpnaucrates
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDatumBoolGPDB
	//
	//	@doc:
	//		GPDB-specific bool representation
	//
	//---------------------------------------------------------------------------
class CDatumBoolGPDB : public IDatumBool
{

	private:

		// type information
		IMDId *m_pmdid;
	
		// boolean value
		BOOL m_fVal;

		// is null
		BOOL m_fNull;

		// private copy ctor
		CDatumBoolGPDB(const CDatumBoolGPDB &);

	public:

		// ctors
		CDatumBoolGPDB(CSystemId sysid, BOOL fVal, BOOL fNull = false);
		CDatumBoolGPDB(IMDId *pmdid, BOOL fVal, BOOL fNull = false);
		
		// dtor
		virtual
		~CDatumBoolGPDB();

		// accessor of metadata type mdid
		virtual
		IMDId *Pmdid() const;

		// accessor of boolean value
		virtual
		BOOL FValue() const;

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
		BOOL FMatch(const IDatum *) const;

		// copy datum
		virtual
		IDatum *PdatumCopy(IMemoryPool *pmp) const;
		
		// print function
		virtual
		IOstream &OsPrint(IOstream &os) const;

	}; // class CDatumBoolGPDB
}

#endif // !GPNAUCRATES_CDatumBoolGPDB_H

// EOF
