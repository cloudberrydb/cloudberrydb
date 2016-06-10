//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWStringDynamic.cpp
//
//	@doc:
//		Implementation of the wide character string class
//		with dynamic buffer allocation.
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/common/CAutoRg.h"
using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::CWStringDynamic
//
//	@doc:
//		Constructs an empty string
//
//---------------------------------------------------------------------------
CWStringDynamic::CWStringDynamic
	(
	IMemoryPool *pmp
	)
	:
	CWString
		(
		0 // ulLength
		),
	m_pmp(pmp),
	m_ulCapacity(0)
{
	Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::CWStringDynamic
//
//	@doc:
//		Constructs a new string and initializes it with the given buffer
//
//---------------------------------------------------------------------------
CWStringDynamic::CWStringDynamic
	(
	IMemoryPool *pmp,
	const WCHAR *wszBuf
	)
	:
	CWString
		(
		GPOS_WSZ_LENGTH(wszBuf)
		),
	m_pmp(pmp),
	m_ulCapacity(0)
{
	GPOS_ASSERT(NULL != wszBuf);

	Reset();
	AppendBuffer(wszBuf);
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::~CWStringDynamic
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CWStringDynamic::~CWStringDynamic()
{
	Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CWString::Reset
//
//	@doc:
//		Resets string
//
//---------------------------------------------------------------------------
void
CWStringDynamic::Reset()
{
	if (NULL != m_wszBuf && &m_wcEmpty != m_wszBuf)
	{
		GPOS_DELETE_ARRAY(m_wszBuf);
	}

	m_wszBuf = const_cast<WCHAR *>(&m_wcEmpty);
	m_ulLength = 0;
	m_ulCapacity = 0;
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendBuffer
//
//	@doc:
//		Appends the contents of a buffer to the current string
//
//---------------------------------------------------------------------------
void
CWStringDynamic::AppendBuffer
	(
	const WCHAR *wsz
	)
{
	GPOS_ASSERT(NULL != wsz);
	ULONG ulLength = GPOS_WSZ_LENGTH(wsz);
	if (0 == ulLength)
	{
		return;
	}

	// expand buffer if needed
	ULONG ulNewLength = m_ulLength + ulLength;
	if (ulNewLength + 1 > m_ulCapacity)
	{
		IncreaseCapacity(ulNewLength);
	}

	clib::WszWcsNCpy(m_wszBuf + m_ulLength, wsz, ulLength + 1);
	m_ulLength = ulNewLength;

	GPOS_ASSERT(FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendWideCharArray
//
//	@doc:
//		Appends a a null terminated wide character array
//
//---------------------------------------------------------------------------
void
CWStringDynamic::AppendWideCharArray
	(
	const WCHAR *wsz
	)
{
	AppendBuffer(wsz);
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendCharArray
//
//	@doc:
//		Appends a a null terminated character array
//
//---------------------------------------------------------------------------
void
CWStringDynamic::AppendCharArray
	(
	const CHAR *sz
	)
{
	GPOS_ASSERT(NULL != sz);

	// expand buffer if needed
	const ULONG ulLength = GPOS_SZ_LENGTH(sz);
	ULONG ulNewLength = m_ulLength + ulLength;
	if (ulNewLength + 1 > m_ulCapacity)
	{
		IncreaseCapacity(ulNewLength);
	}
	WCHAR *wszBuf = GPOS_NEW_ARRAY(m_pmp, WCHAR, ulLength + 1);

	// convert input string to wide character buffer
#ifdef GPOS_DEBUG
	ULONG ulLen =
#endif // GPOS_DEBUG
		clib::UlMbToWcs(wszBuf, sz, ulLength);
	GPOS_ASSERT(ulLen == ulLength);

	// append input string to current end of buffer
	(void) clib::WszWMemCpy(m_wszBuf + m_ulLength, wszBuf, ulLength + 1);
	GPOS_DELETE_ARRAY(wszBuf);

	m_wszBuf[ulNewLength] = WCHAR_EOS;
	m_ulLength = ulNewLength;

	GPOS_ASSERT(FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendFormat
//
//	@doc:
//		Appends a formatted string
//
//---------------------------------------------------------------------------
void
CWStringDynamic::AppendFormat
	(
	const WCHAR *wszFormat,
	...
	)
{
	GPOS_ASSERT(NULL != wszFormat);
	using clib::IVswPrintf;

	VA_LIST	vaArgs;

	// determine length of format string after expansion
	INT iRes = -1;

	// attempt to fit the formatted string in a static array
	WCHAR wszBufStatic[GPOS_WSTR_DYNAMIC_STATIC_BUFFER];

	// get arguments
	VA_START(vaArgs, wszFormat);

	// try expanding the formatted string in the buffer
	iRes = IVswPrintf(wszBufStatic, GPOS_ARRAY_SIZE(wszBufStatic), wszFormat, vaArgs);

	// reset arguments
	VA_END(vaArgs);
	GPOS_ASSERT(-1 <= iRes);

	// estimated number of characters in expanded format string
	ULONG ulSize = std::max(GPOS_WSZ_LENGTH(wszFormat), GPOS_ARRAY_SIZE(wszBufStatic));

	// if the static buffer is too small, find the formatted string
	// length by trying to store it in a buffer of increasing size
	while (-1 == iRes)
	{
		// try with a bigger buffer this time
		ulSize *= 2;
		CAutoRg<WCHAR> a_wszBuf;
		a_wszBuf = GPOS_NEW_ARRAY(m_pmp, WCHAR, ulSize + 1);

		// get arguments
		VA_START(vaArgs, wszFormat);

		// try expanding the formatted string in the buffer
		iRes = IVswPrintf(a_wszBuf.Rgt(), ulSize, wszFormat, vaArgs);

		// reset arguments
		VA_END(vaArgs);

		GPOS_ASSERT(-1 <= iRes);
	}
	// verify required buffer was not bigger than allowed
	GPOS_ASSERT(iRes >= 0);

	// expand buffer if needed
	ULONG ulNewLength = m_ulLength + ULONG(iRes);
	if (ulNewLength + 1 > m_ulCapacity)
	{
		IncreaseCapacity(ulNewLength);
	}

	// get arguments
	VA_START(vaArgs, wszFormat);

	// print vaArgs to string
	IVswPrintf(m_wszBuf + m_ulLength, iRes + 1, wszFormat, vaArgs);

	// reset arguments
	VA_END(vaArgs);

	m_ulLength = ulNewLength;
	GPOS_ASSERT(FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::AppendEscape
//
//	@doc:
//		Appends a string and replaces character with string
//
//---------------------------------------------------------------------------
void
CWStringDynamic::AppendEscape
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

	// count how many times the character to be escaped appears in the string
	ULONG ulOccurences = pstr->UlOccurences(wc);
	if (0 == ulOccurences)
	{
		Append(pstr);
		return;
	}

	ULONG ulLength = pstr->UlLength();
	const WCHAR * wsz = pstr->Wsz();

	ULONG ulLengthReplace =  GPOS_WSZ_LENGTH(wszReplace);
	ULONG ulNewLength = m_ulLength + ulLength + (ulLengthReplace - 1) * ulOccurences;
	if (ulNewLength + 1 > m_ulCapacity)
	{
		IncreaseCapacity(ulNewLength);
	}

	// append new contents while replacing character with escaping string
	for (ULONG i = 0, j = m_ulLength; i < ulLength; i++)
	{
		if (wc == wsz[i] && !pstr->FEscaped(i))
		{
			clib::WszWcsNCpy(m_wszBuf + j, wszReplace, ulLengthReplace);
			j += ulLengthReplace;
		}
		else
		{
			m_wszBuf[j++] = wsz[i];
		}
	}

	// terminate string
	m_wszBuf[ulNewLength] = WCHAR_EOS;
	m_ulLength = ulNewLength;

	GPOS_ASSERT(FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::IncreaseCapacity
//
//	@doc:
//		Increase string capacity
//
//---------------------------------------------------------------------------
void
CWStringDynamic::IncreaseCapacity
	(
	ULONG ulRequested
	)
{
	GPOS_ASSERT(ulRequested + 1 > m_ulCapacity);

	ULONG ulCapacity = UlCapacity(ulRequested + 1);
	GPOS_ASSERT(ulCapacity > ulRequested + 1);
	GPOS_ASSERT(ulCapacity >= (m_ulCapacity << 1));

	CAutoRg<WCHAR> a_wszNewBuf;
	a_wszNewBuf = GPOS_NEW_ARRAY(m_pmp, WCHAR, ulCapacity);
	if (0 < m_ulLength)
	{
		// current string is not empty: copy it to the resulting string
		a_wszNewBuf = clib::WszWcsNCpy(a_wszNewBuf.Rgt(), m_wszBuf, m_ulLength);
	}

	// release old buffer
	if (m_wszBuf != &m_wcEmpty)
	{
		GPOS_DELETE_ARRAY(m_wszBuf);
	}
	m_wszBuf = a_wszNewBuf.RgtReset();
	m_ulCapacity = ulCapacity;
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringDynamic::UlCapacity
//
//	@doc:
//		Find capacity that fits requested string size
//
//---------------------------------------------------------------------------
ULONG
CWStringDynamic::UlCapacity
	(
	ULONG ulRequested
	)
{
	ULONG ulCapacity = GPOS_WSTR_DYNAMIC_CAPACITY_INIT;
	while (ulCapacity <= ulRequested + 1)
	{
		ulCapacity = ulCapacity << 1;
	}

	return ulCapacity;
}


// EOF

