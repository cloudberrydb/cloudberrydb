//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010, Greenplum, Inc.
//
//	@filename:
//		CMessageRepository.h
//
//	@doc:
//		Error message repository; 
//---------------------------------------------------------------------------
#ifndef GPOS_CMessageRepository_H
#define GPOS_CMessageRepository_H

#include "gpos/sync/CSpinlock.h"
#include "gpos/error/CMessageTable.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMessageRepository
	//
	//	@doc:
	//		Stores and loads all error messages by locale
	//
	//---------------------------------------------------------------------------
	class CMessageRepository
	{
		private:
		
			// global singleton
			static
			CMessageRepository *m_pmr;
			
			// memory pool
			IMemoryPool *m_pmp;

			// short hand for Table of Message Tables (TMT)
			typedef CSyncHashtable<
						CMessageTable, 
						ELocale, 
						CSpinlockOS> TMT;

			// short hand for TMT accessor
			typedef CSyncHashtableAccessByKey<
						CMessageTable, 
						ELocale, 
						CSpinlockOS> TMTAccessor;
		
			// basic hash table
			TMT m_tmt;
			
			// init basic directory
			void InitDirectory(IMemoryPool *pmp);
			
			// install message table for a given locale
			void AddMessageTable(ELocale eloc);
		
			// pre-load standard messages
			void LoadStandardMessages();

		public:

			// ctor
			CMessageRepository(IMemoryPool *pmp);

			// dtor
			~CMessageRepository();

			// lookup message by error/local
			CMessage *PmsgLookup(CException exc, ELocale eloc);

			// add individual message
			void AddMessage(ELocale eloc, CMessage *pmsg);

			// initializer for global singleton
			static
			GPOS_RESULT EresInit();

			// accessor for global singleton
			static 
			CMessageRepository* Pmr();

			void Shutdown();

	}; // class CMessageRepository
}

#endif // !GPOS_CMessageRepository_H

// EOF

