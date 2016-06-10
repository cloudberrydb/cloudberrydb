//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CTaskLocalStorage.cpp
//
//	@doc:
//		Task-local storage implementation
//---------------------------------------------------------------------------

#include "gpos/task/CTaskLocalStorage.h"
#include "gpos/task/CTaskLocalStorageObject.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"

using namespace gpos;


// shorthand for HT accessor
typedef	CSyncHashtableAccessByKey
			<CTaskLocalStorageObject, 
			CTaskLocalStorage::Etlsidx, 
			CSpinlockOS> ShtAcc;


// invalid Etlsidx
const CTaskLocalStorage::Etlsidx CTaskLocalStorage::m_etlsidxInvalid = EtlsidxInvalid;


//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::~CTaskLocalStorage
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CTaskLocalStorage::~CTaskLocalStorage() {}

//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::Reset
//
//	@doc:
//		(re-)init TLS
//
//---------------------------------------------------------------------------
void
CTaskLocalStorage::Reset
	(
	IMemoryPool *pmp
	)
{
	// destroy old 
	m_sht.Cleanup();

	// realloc
	m_sht.Init
		(
		pmp,
		128, // number of hashbuckets
		GPOS_OFFSET(CTaskLocalStorageObject, m_link),
		GPOS_OFFSET(CTaskLocalStorageObject, m_etlsidx),
		&(CTaskLocalStorage::m_etlsidxInvalid),
		CTaskLocalStorage::UlHashIdx,
		CTaskLocalStorage::FEqual
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::Store
//
//	@doc:
//		Store object in TLS
//
//---------------------------------------------------------------------------
void
CTaskLocalStorage::Store
	(
	CTaskLocalStorageObject *ptlsobj
	)
{
	GPOS_ASSERT(NULL != ptlsobj);

#ifdef GPOS_DEBUG
	{
		ShtAcc shtacc(m_sht, ptlsobj->Etlsidx());
		GPOS_ASSERT(NULL == shtacc.PtLookup() && "Duplicate TLS object key");
	}
#endif // GPOS_DEBUG

	m_sht.Insert(ptlsobj);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::Ptlsobj
//
//	@doc:
//		Lookup object in TLS
//
//---------------------------------------------------------------------------
CTaskLocalStorageObject *
CTaskLocalStorage::Ptlsobj
	(
	CTaskLocalStorage::Etlsidx etlsidx
	)
{
	ShtAcc shtacc(m_sht, etlsidx);
	return shtacc.PtLookup();
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskLocalStorage::Remove
//
//	@doc:
//		Delete given object from TLS
//
//---------------------------------------------------------------------------
void
CTaskLocalStorage::Remove
	(
	CTaskLocalStorageObject *ptlsobj
	)
{
	GPOS_ASSERT(NULL != ptlsobj);
	
	// lookup object
	ShtAcc shtacc(m_sht, ptlsobj->Etlsidx());
	GPOS_ASSERT(NULL != shtacc.PtLookup() && "Object not found in TLS");
	
	shtacc.Remove(ptlsobj);
}

// EOF

