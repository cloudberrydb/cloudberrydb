//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010, Greenplum, Inc.
//
//	@filename:
//		CMessageTable.h
//
//	@doc:
//		Error message table; 
//---------------------------------------------------------------------------
#ifndef GPOS_CMessageTable_H
#define GPOS_CMessageTable_H

#include "gpos/error/CMessage.h"

#define GPOS_MSGTAB_SIZE	4096

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMessageTable
	//
	//	@doc:
	//		Maintains error messages for a given locale
	//
	//---------------------------------------------------------------------------
	class CMessageTable
	{
			// short hand for message tables
			typedef CSyncHashtable<
						CMessage, 
						CException, 
						CSpinlockOS> MT;

			// short hand for message table accessor
			typedef CSyncHashtableAccessByKey<
						CMessage, 
						CException, 
						CSpinlockOS> MTAccessor;
		
			// message hashtable
			MT m_sht;
		
			// private copy ctor
			CMessageTable(const CMessageTable&);
			
		public:

			// ctor
			CMessageTable(IMemoryPool *pmp, ULONG ulSize, ELocale eloc);
		
			// lookup message by error/local
			CMessage *PmsgLookup(CException exc);
			
			// insert message
			void AddMessage(CMessage *pmsg);

			// simple comparison
			BOOL operator ==
				(
				const CMessageTable &mt
				)
				const
			{
				return m_eloc == mt.m_eloc;
			}
			
			// equality function -- needed for hashtable
			static
			BOOL FEqual
				(
				const ELocale &eloc,
				const ELocale &elocOther
				)
			{
				return eloc == elocOther;
			}

			// basic hash function
			static
			ULONG UlHash
				(
				const ELocale &eloc
				)
			{
				return (ULONG) eloc;
			}

			// link object
			SLink m_link;

			// locale
			ELocale m_eloc;

			// invalid locale
			static
			const ELocale m_elocInvalid;
					
	}; // class CMessageTable
}

#endif // !GPOS_CMessageTable_H

// EOF

