//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CMessage.h
//
//	@doc:
//		Error message container; each instance corresponds to a message as
//		loaded from an external configuration file;
//		Both warnings and errors;
//---------------------------------------------------------------------------
#ifndef GPOS_CMessage_H
#define GPOS_CMessage_H



#include "gpos/types.h"
#include "gpos/assert.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CSyncHashtable.h"

#define GPOS_WSZ_WSZLEN(x)    (L##x), (gpos::clib::UlWcsLen(L##x))

#define GPOS_ERRMSG_FORMAT(...) \
	gpos::CMessage::FormatMessage(__VA_ARGS__)

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CMessage
	//
	//	@doc:
	//		Corresponds to individual message as defined in config file
	//
	//---------------------------------------------------------------------------
	class CMessage
	{
		private:

			// severity
			ULONG m_ulSev;
			
			// format string
			const WCHAR *m_wszFmt;
			
			// length of format string
			ULONG m_ulLenFmt;

			// number of parameters
			ULONG m_ulParams;
			
			// comment string
			const WCHAR *m_wszComment;
			
			// length of commen string
			ULONG m_ulLenComment;

		public:
		
			// exception carries error number/identification
			CException m_exc;

			// TODO: 6/29/2010: incorporate string class
			// as soon as available
			//
			// ctor
			CMessage(CException exc, 
					 ULONG ulSev,
					 const WCHAR *wszFmt, ULONG ulLenFmt, 
					 ULONG ulParams,
					 const WCHAR *wszComment, ULONG ulLenComment);

			// copy ctor
			CMessage(const CMessage&);

			// format contents into given buffer
			void
			Format(CWStringStatic *pwss, VA_LIST) const;

			// severity accessor
			ULONG UlSev() const
			{
				return m_ulSev;
			}

			// link object
			SLink m_link;

			// access a message by index
			static
			CMessage *Pmsg(ULONG ulIndex);

			// format an error message
			static 
			void FormatMessage(CWStringStatic *pstr, ULONG ulMajor, ULONG ulMinor, ...);
			
#ifdef GPOS_DEBUG
			// debug print function
			IOstream &
			OsPrint(IOstream &);
#endif // GPOS_DEBUG

	}; // class CMessage
}

#endif // !GPOS_CMessage_H

// EOF

