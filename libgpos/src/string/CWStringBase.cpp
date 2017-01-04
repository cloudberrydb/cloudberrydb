//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CWStringBase.cpp
//
//	@doc:
//		Implementation of the base abstract wide character string class
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/string/CWStringBase.h"
#include "gpos/string/CWStringConst.h"

using namespace gpos;

const WCHAR CWStringBase::m_wcEmpty = GPOS_WSZ_LIT('\0');

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::PStrCopy
//
//	@doc:
//		Creates a deep copy of the string
//
//---------------------------------------------------------------------------
CWStringConst *CWStringBase::PStrCopy
	(
	IMemoryPool *pmp
	) const
{
	return GPOS_NEW(pmp) CWStringConst(pmp, Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::FValid
//
//	@doc:
//		Checks if the string is properly NULL-terminated
//
//---------------------------------------------------------------------------
bool
CWStringBase::FValid() const
{
	return (UlLength() == GPOS_WSZ_LENGTH(Wsz()));
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::operator == 
//
//	@doc:
//		Equality operator on strings
//
//---------------------------------------------------------------------------
BOOL
CWStringBase::operator ==
	(
	const CWStringBase &str
	)
	const
{
	return FEquals(&str);
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::UlLength()
//
//	@doc:
//		Returns the length of the string in number of wide characters,
//		not counting the terminating '\0'
//
//---------------------------------------------------------------------------
ULONG
CWStringBase::UlLength() const
{
	return m_ulLength;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::FEquals
//
//	@doc:
//		Checks whether the string is byte-wise equal to another string
//
//---------------------------------------------------------------------------
BOOL
CWStringBase::FEquals
	(
	const CWStringBase *pStr
	)
	const
{
	GPOS_ASSERT(NULL != pStr);
	return FEquals(pStr->Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::FEquals
//
//	@doc:
//		Checks whether the string is byte-wise equal to a string literal
//
//---------------------------------------------------------------------------
BOOL
CWStringBase::FEquals
	(
	const WCHAR *wszBuf
	)
	const
{
	GPOS_ASSERT(NULL != wszBuf);
	ULONG ulLength = GPOS_WSZ_LENGTH(wszBuf);
	if (UlLength() == ulLength &&
		0 == clib::IWcsNCmp(Wsz(), wszBuf, ulLength))
	{
		return true;
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::FEmpty
//
//	@doc:
//		Checks whether the string is empty
//
//---------------------------------------------------------------------------
BOOL
CWStringBase::FEmpty() const
{
	return (0 == UlLength());
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::IFind
//
//	@doc:
//		Returns the index of the first occurrence of a character, -1 if not found
//
//---------------------------------------------------------------------------
INT
CWStringBase::IFind
	(
	WCHAR wc
	)
	const
{
	const WCHAR *wsz = Wsz();
	const ULONG ulLength = UlLength();

	for (ULONG i = 0; i < ulLength; i++)
	{
		if (wc == wsz[i])
		{
			return i;
		}
	}

	return -1;
}


//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::FEscaped
//
//	@doc:
//		Checks if a character is escaped
//
//---------------------------------------------------------------------------
BOOL
CWStringBase::FEscaped
	(
	ULONG ulOfst
	)
	const
{
	GPOS_ASSERT(!FEmpty());
	GPOS_ASSERT(UlLength() > ulOfst);

	const WCHAR *wszBuf = Wsz();

	for (ULONG i = ulOfst; i > 0; i--)
	{
		// check for escape character
		if (GPOS_WSZ_LIT('\\') != wszBuf[i - 1])
		{
			if (0 == ((ulOfst - i) & ULONG(1)))
			{
				return false;
			}
			else
			{
				return true;
			}
		}
	}

	// reached beginning of string
	if (0 == (ulOfst & ULONG(1)))
	{
		return false;
	}
	else
	{
		return true;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CWStringBase::UlOccurences
//
//	@doc:
//		Count how many times the character appears in string
//
//---------------------------------------------------------------------------
ULONG
CWStringBase::UlOccurences
	(
	const WCHAR wc
	) const
{
	ULONG ulOccurences = 0;
	ULONG ulLength = UlLength();
	const WCHAR *wsz = Wsz();

	for (ULONG i = 0; i < ulLength; i++)
	{
		if (wc == wsz[i] && !FEscaped(i))
		{
			ulOccurences++;
		}
	}
	return ulOccurences;
}
// EOF

