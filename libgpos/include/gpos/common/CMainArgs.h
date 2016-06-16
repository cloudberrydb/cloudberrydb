//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CMainArgs.h
//
//	@doc:
//		simple wrapper to pass standard args from main to other routines
//---------------------------------------------------------------------------
#ifndef GPOS_CMainArgs_H
#define GPOS_CMainArgs_H

#include "gpos/types.h"


namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CMainArgs
	//
	//	@doc:
	//		Main args, following standard convention int, char**
	//
	//---------------------------------------------------------------------------
	class CMainArgs
	{
		private:
			
			// number of arguments
			ULONG m_ulArgs;
			
			// pointer to string array
			const CHAR **m_rgszArgs;
			
			// format string
			const CHAR *m_szFmt;

			// saved option params
			CHAR *m_szOptarg;
			INT m_iOptind;
			INT m_iOptopt;
			INT m_iOpterr;
#ifdef GPOS_Darwin
			INT m_iOptreset;
#endif // GPOS_Darwin

		public:

			// ctor
			CMainArgs(ULONG ulArgs, const CHAR **rgszArgs, const CHAR *szFmt);

			// dtor -- restores option params
			~CMainArgs();
			
			// getopt functionality
			BOOL FGetopt(CHAR *ch);
			
	}; // class CMainArgs
}

#endif // GPOS_CMainArgs_H

// EOF

