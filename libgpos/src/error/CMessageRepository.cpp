//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CMessageRepository.cpp
//
//	@doc:
//		Singleton to keep error messages;
//---------------------------------------------------------------------------

#include "gpos/utils.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/error/CMessageRepository.h"
#include "gpos/memory/CAutoMemoryPool.h"


using namespace gpos;
//---------------------------------------------------------------------------
// static singleton
//---------------------------------------------------------------------------
CMessageRepository *CMessageRepository::m_pmr = NULL;

//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::CMessageRepository
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CMessageRepository::CMessageRepository
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{
	GPOS_ASSERT(NULL != pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::CMessageRepository
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CMessageRepository::~CMessageRepository()
{
	// no explicit cleanup;
	// shutdown routine will reclaim all memory
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::PmsgLookup
//
//	@doc:
//		Lookup a message by a given CException/Local combination
//
//---------------------------------------------------------------------------
CMessage *
CMessageRepository::PmsgLookup
	(
	CException exc,
	ELocale eloc
	)
{
	GPOS_ASSERT(exc != CException::m_excInvalid &&
				"Cannot lookup invalid exception message");

	if (exc != CException::m_excInvalid)
	{
		CMessage *pmsg = NULL;
		ELocale elocSearch = eloc;

		for (ULONG i = 0; i < 2; i++)
		{
			// try to locate locale-specific message table
			TMTAccessor tmta(m_tmt, elocSearch);
			CMessageTable *pmt = tmta.PtLookup();
		
			if (NULL != pmt)
			{
				// try to locate specific message
				pmsg = pmt->PmsgLookup(exc);
				if (NULL != pmsg)
				{
					return pmsg;
				}
			}

			// retry with en-US locale
			elocSearch = ElocEnUS_Utf8;
		}
	}

	return CMessage::Pmsg(CException::ExmiInvalid);
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::EresInit
//
//	@doc:
//		Initialize global instance of message repository
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMessageRepository::EresInit()
{
	GPOS_ASSERT(NULL == m_pmr);
	
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CMessageRepository *pmr = GPOS_NEW(pmp) CMessageRepository(pmp);
	pmr->InitDirectory(pmp);
	pmr->LoadStandardMessages();
	
	CMessageRepository::m_pmr = pmr;

	// detach safety
	(void) amp.PmpDetach();
	
	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::Pmr
//
//	@doc:
//		Retrieve singleton
//
//---------------------------------------------------------------------------
CMessageRepository *
CMessageRepository::Pmr()
{
	GPOS_ASSERT(NULL != m_pmr);
	return m_pmr;
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::Shutdown
//
//	@doc:
//		Reclaim memory -- no specific clean up operation
//		Does not reclaim memory of messages; that remains the loader's
//		responsibility
//
//---------------------------------------------------------------------------
void
CMessageRepository::Shutdown()
{
	CMemoryPoolManager::Pmpm()->Destroy(m_pmp);
	CMessageRepository::m_pmr = NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::InitDirectory
//
//	@doc:
//		Install table-of-tables directory
//
//---------------------------------------------------------------------------
void
CMessageRepository::InitDirectory
	(
	IMemoryPool *pmp
	)
{
	m_tmt.Init
		(
		pmp,
		128,
		GPOS_OFFSET(CMessageTable, m_link),
		GPOS_OFFSET(CMessageTable, m_eloc),
		&(CMessageTable::m_elocInvalid),
		CMessageTable::UlHash,
		CMessageTable::FEqual
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::AddMessage
//
//	@doc:
//		Add individual message to a locale-specific message table; create
//		table on demand as needed
//
//---------------------------------------------------------------------------
void
CMessageRepository::AddMessage
	(
	ELocale eloc, 
	CMessage *pmsg
	)
{
	// retry logic: (1) attempt to insert first (frequent code path)
	// or (2) create message table after failure and retry (infreq code path)
	for (ULONG i = 0; i < 2; i++)
	{
		// scope for accessor lock
		{
			TMTAccessor tmta(m_tmt, eloc);
			CMessageTable *pmt = tmta.PtLookup();
			
			if (NULL != pmt)
			{
				pmt->AddMessage(pmsg);
				return;
			}
		}

		// create message table for this locale on demand
		AddMessageTable(eloc);					
	}
	
	GPOS_ASSERT(!"Adding message table on demand failed");
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::AddMessageTable
//
//	@doc:
//		Add new locale table
//
//---------------------------------------------------------------------------
void
CMessageRepository::AddMessageTable
	(
	ELocale eloc
	)
{
	CMessageTable *pmtNew = 
		GPOS_NEW(m_pmp) CMessageTable(m_pmp, GPOS_MSGTAB_SIZE, eloc);
	
	{
		TMTAccessor tmta(m_tmt, eloc);
		CMessageTable *pmt = tmta.PtLookup();

		if (NULL == pmt)
		{
			tmta.Insert(pmtNew);
			return;
		}
	}

	GPOS_DELETE(pmtNew);
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::LoadStandardMessages
//
//	@doc:
//		Insert standard messages for enUS locale;
//
//---------------------------------------------------------------------------
void
CMessageRepository::LoadStandardMessages()
{
	for (ULONG ul = 0; ul < CException::ExmiSentinel; ul++)
	{
		CMessage *pmsg = CMessage::Pmsg(ul);
		if (CException::m_excInvalid != pmsg->m_exc)
		{
			AddMessage(ElocEnUS_Utf8, pmsg);
		}
	}
}

// EOF

