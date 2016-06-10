//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CWStringStatic.h
//
//	@doc:
//		Wide character String class with buffer.
//---------------------------------------------------------------------------
#ifndef GPOS_CWStringStatic_H
#define GPOS_CWStringStatic_H

#include "gpos/base.h"
#include "gpos/string/CWString.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CWStringStatic
	//
	//	@doc:
	//		Implementation of the string interface with buffer pre-allocation.
	//		Internally, the class uses a null-terminated WCHAR buffer to store the string
	//		characters.	The buffer is assigned at construction time; its capacity cannot be
	//		modified, thus restricting the maximum size of the stored string. Attempting to
	//		store a larger string than the available buffer capacity results in truncation.
	//		CWStringStatic owner is responsible for allocating the buffer and releasing it
	//		after the object is destroyed.
	//
	//---------------------------------------------------------------------------
	class CWStringStatic : public CWString
	{
		private:

			// buffer capacity
			ULONG m_ulCapacity;

			// private copy ctor
			CWStringStatic(const CWStringStatic&);
			
		protected:

			// appends the contents of a buffer to the current string
			void AppendBuffer(const WCHAR *wszBuf);
			
		public:

			// ctor
			CWStringStatic(WCHAR wszBuffer[], ULONG ulCapacity);

			// ctor with string initialization
			CWStringStatic(WCHAR wszBuffer[], ULONG ulCapacity, const WCHAR wszInit[]);

			// appends a string and replaces character with string
			void AppendEscape(const CWStringBase *pstr, WCHAR wc, const WCHAR *wszReplace);

			// appends a formatted string
			void AppendFormat(const WCHAR *wszFormat, ...);

			// appends a formatted string based on passed va list
			void AppendFormatVA(const WCHAR *wszFormat, VA_LIST vaArgs);

			// appends a null terminated character array
			virtual
			void AppendCharArray(const CHAR *sz);

			// appends a null terminated  wide character array
			virtual
			void AppendWideCharArray(const WCHAR *wsz);

			// dtor - owner is responsible for releasing the buffer
			virtual ~CWStringStatic()
			{}
					
			// resets string
			void Reset();

	};
}

#endif // !GPOS_CWStringStatic_H

// EOF

