//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWStringStatic.cpp
//
//	@doc:
//		Implementation of the wide character string class
//		with static buffer allocation
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/string/CWStringStatic.h"

using namespace gpos;

#define GPOS_STATIC_STR_BUFFER_LENGTH 5000


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::CWStringStatic
//
//	@doc:
//		Ctor - initializes with empty string
//
//---------------------------------------------------------------------------
CWStringStatic::CWStringStatic
	(
	WCHAR wszBuffer[],
	ULONG ulCapacity
	)
	:
	CWString
		(
		0 // ulLength
		),
	m_ulCapacity(ulCapacity)
{
	GPOS_ASSERT(NULL != wszBuffer);
	GPOS_ASSERT(0 < m_ulCapacity);

	m_wszBuf = wszBuffer;
	m_wszBuf[0] = WCHAR_EOS;
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::CWStringStatic
//
//	@doc:
//		Ctor - initializes with passed string
//
//---------------------------------------------------------------------------
CWStringStatic::CWStringStatic
	(
	WCHAR wszBuffer[],
	ULONG ulCapacity,
	const WCHAR wszInit[]
	)
	:
	CWString
		(
		0 // ulLength
		),
	m_ulCapacity(ulCapacity)
{
	GPOS_ASSERT(NULL != wszBuffer);
	GPOS_ASSERT(0 < m_ulCapacity);

	m_wszBuf = wszBuffer;
	AppendBuffer(wszInit);
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendBuffer
//
//	@doc:
//		Appends the contents of a buffer to the current string
//
//---------------------------------------------------------------------------
void
CWStringStatic::AppendBuffer
	(
	const WCHAR *wszBuf
	)
{
	GPOS_ASSERT(NULL != wszBuf);
	ULONG ulLength = GPOS_WSZ_LENGTH(wszBuf);
	if (0 == ulLength || m_ulCapacity == m_ulLength)
	{
		return;
	}
	
	// check if new length exceeds capacity
	if (m_ulCapacity <= ulLength + m_ulLength)
	{
		// truncate string
		ulLength = m_ulCapacity - m_ulLength - 1;
	}

	GPOS_ASSERT(m_ulCapacity > ulLength + m_ulLength);

	clib::WszWcsNCpy(m_wszBuf + m_ulLength, wszBuf, ulLength + 1);
	m_ulLength += ulLength;
	
	// terminate string
	m_wszBuf[m_ulLength] = WCHAR_EOS;

	GPOS_ASSERT(FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendWideCharArray
//
//	@doc:
//		Appends a null terminated wide character array
//
//---------------------------------------------------------------------------
void
CWStringStatic::AppendWideCharArray
	(
	const WCHAR *wsz
	)
{
	AppendBuffer(wsz);
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendCharArray
//
//	@doc:
//		Appends a null terminated character array
//
//---------------------------------------------------------------------------
void
CWStringStatic::AppendCharArray
	(
	const CHAR *sz
	)
{
	GPOS_ASSERT(NULL != sz);
	if (0 ==  GPOS_SZ_LENGTH(sz) || m_ulCapacity == m_ulLength)
	{
		return;
	}

	ULONG ulLength = GPOS_SZ_LENGTH(sz);
	if (ulLength >= GPOS_STATIC_STR_BUFFER_LENGTH)
	{
		// if input string is larger than buffer length, use AppendFormat
		AppendFormat(GPOS_WSZ_LIT("%s"), sz);
		return;
	}

	// otherwise, append to wide string character array directly
	WCHAR wszBuf[GPOS_STATIC_STR_BUFFER_LENGTH];

	// convert input string to wide character buffer
	#ifdef GPOS_DEBUG
	ULONG ulLen =
	#endif // GPOS_DEBUG
		clib::UlMbToWcs(wszBuf, sz, ulLength);
	GPOS_ASSERT(ulLen == ulLength);

	// check if new length exceeds capacity
	if (m_ulCapacity <= ulLength + m_ulLength)
	{
		// truncate string
		ulLength = m_ulCapacity - m_ulLength - 1;
	}
	GPOS_ASSERT(m_ulCapacity > ulLength + m_ulLength);

	// append input string to current end of buffer
	(void) clib::WszWMemCpy(m_wszBuf + m_ulLength, wszBuf, ulLength + 1);

	m_ulLength += ulLength;
	m_wszBuf[m_ulLength] = WCHAR_EOS;

	GPOS_ASSERT(FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendFormat
//
//	@doc:
//		Appends a formatted string
//
//---------------------------------------------------------------------------
void
CWStringStatic::AppendFormat
	(
	const WCHAR *wszFormat,
	...
	)
{
	VA_LIST	vaArgs;

	// get arguments
	VA_START(vaArgs, wszFormat);

	AppendFormatVA(wszFormat, vaArgs);

	// reset arguments
	VA_END(vaArgs);
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendFormatVA
//
//	@doc:
//		Appends a formatted string based on passed va list
//
//---------------------------------------------------------------------------
void
CWStringStatic::AppendFormatVA
	(
	const WCHAR *wszFormat,
	VA_LIST vaArgs
	)
{
	GPOS_ASSERT(NULL != wszFormat);

	// available space in buffer
	ULONG ulAvailable = m_ulCapacity - m_ulLength;

	// format string
	(void) clib::IVswPrintf(m_wszBuf + m_ulLength, ulAvailable, wszFormat, vaArgs);

	m_wszBuf[m_ulCapacity - 1] = WCHAR_EOS;
	m_ulLength = GPOS_WSZ_LENGTH(m_wszBuf);

	GPOS_ASSERT(m_ulCapacity > m_ulLength);

	GPOS_ASSERT(FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::Reset
//
//	@doc:
//		Resets string
//
//---------------------------------------------------------------------------
void
CWStringStatic::Reset()
{
	m_wszBuf[0] = WCHAR_EOS;
	m_ulLength = 0;
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringStatic::AppendEscape
//
//	@doc:
//		Appends a string and replaces character with string
//
//---------------------------------------------------------------------------
void
CWStringStatic::AppendEscape
	(
	const CWStringBase *pstr,
	WCHAR wc,
	const WCHAR *wszReplace
	)
{
	GPOS_ASSERT(NULL != pstr);

	if (pstr->FEmpty())
	{
		return;
	}

	ULONG ulLength = pstr->UlLength();
	ULONG ulLengthReplace =  GPOS_WSZ_LENGTH(wszReplace);
	ULONG ulLengthNew = m_ulLength;
	const WCHAR *wsz = pstr->Wsz();

	for (ULONG i = 0; i < ulLength && ulLengthNew < m_ulCapacity - 1; i++)
	{
		if (wc == wsz[i] && !pstr->FEscaped(i))
		{
			// check for overflow
			ULONG ulLengthCopy = std::min(ulLengthReplace, m_ulCapacity - ulLengthNew - 1);

			clib::WszWcsNCpy(m_wszBuf + ulLengthNew, wszReplace, ulLengthCopy);
			ulLengthNew += ulLengthCopy;
		}
		else
		{
			m_wszBuf[ulLengthNew] = wsz[i];
			ulLengthNew++;
		}

		GPOS_ASSERT(ulLengthNew < m_ulCapacity);
	}

	// terminate string
	m_wszBuf[ulLengthNew] = WCHAR_EOS;

	m_ulLength = ulLengthNew;
	GPOS_ASSERT(FValid());
}


// EOF

