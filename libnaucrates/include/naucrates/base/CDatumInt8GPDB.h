//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDatumInt8GPDB.h
//
//	@doc:
//		GPDB-specific Int8 representation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumInt8GPDB_H
#define GPNAUCRATES_CDatumInt8GPDB_H

#include "gpos/base.h"

#include "naucrates/base/IDatumInt8.h"

#include "naucrates/md/CMDTypeInt8GPDB.h"

namespace gpnaucrates
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDatumInt8GPDB
	//
	//	@doc:
	//		GPDB-specific Int8 representation
	//
	//---------------------------------------------------------------------------
	class CDatumInt8GPDB : public IDatumInt8
	{

		private:

			// type information
			IMDId *m_pmdid;
		
			// integer value
			LINT m_lVal;

			// is null
			BOOL m_fNull;


			// private copy ctor
			CDatumInt8GPDB(const CDatumInt8GPDB &);

		public:

			// ctors
			CDatumInt8GPDB(CSystemId sysid, LINT lVal, BOOL fNull = false);
			CDatumInt8GPDB(IMDId *pmdid, LINT lVal, BOOL fNull = false);

			// dtor
			virtual
			~CDatumInt8GPDB();

			// accessor of metadata type id
			virtual
			IMDId *Pmdid() const;

			// accessor of size
			virtual
			ULONG UlSize() const;

			// accessor of integer value
			virtual
			LINT LValue() const;

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

	}; // class CDatumInt8GPDB

}


#endif // !GPNAUCRATES_CDatumInt8GPDB_H

// EOF
