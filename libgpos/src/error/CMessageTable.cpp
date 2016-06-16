//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CMessageTable.cpp
//
//	@doc:
//		Implements message tables
//---------------------------------------------------------------------------

#include "gpos/utils.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/error/CMessageTable.h"
#include "gpos/common/clibwrapper.h"

using namespace gpos;

// invalid locale
const ELocale CMessageTable::m_elocInvalid = ELocInvalid;


//---------------------------------------------------------------------------
//	@function:
//		CMessageTable::CMessageTable
//
//	@doc:
//
//---------------------------------------------------------------------------
CMessageTable::CMessageTable
	(
	IMemoryPool *pmp,
	ULONG ulSize,
	ELocale eloc
	)
	:
	m_eloc(eloc)
{
	m_sht.Init
		(
		pmp,
		ulSize,
		GPOS_OFFSET(CMessage, m_link),
		GPOS_OFFSET(CMessage, m_exc),
		&(CException::m_excInvalid),
		CException::UlHash,
		CException::FEqual
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageTable::PmsgLookup
//
//	@doc:
//		Lookup message 
//
//---------------------------------------------------------------------------
CMessage*
CMessageTable::PmsgLookup
	(
	CException exc
	)
{
	MTAccessor shta(m_sht, exc);
	return shta.PtLookup();
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageTable::AddMessage
//
//	@doc:
//		Insert new message
//
//---------------------------------------------------------------------------
void
CMessageTable::AddMessage
	(
	CMessage *pmsg
	)
{
	MTAccessor shta(m_sht, pmsg->m_exc);

	if (NULL == shta.PtLookup())
	{
		shta.Insert(pmsg);
	}
	
	// TODO: 6/24/2010; raise approp. error for duplicate message
	// or simply ignore?
}

// EOF

