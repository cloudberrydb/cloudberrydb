//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2010 Greenplum, Inc.
//
//	@filename:
//		COstream.cpp
//
//	@doc:
//		Implementation of basic wide character output stream
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"
#include "gpos/io/COstream.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		COstream::COstream
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COstream::COstream
    (
    )
	:
    m_wss(m_wsz, GPOS_OSTREAM_CONVBUF_SIZE),
    m_esm(EsmDec)
{}


IOstream &
COstream::AppendFormat(const WCHAR *wszFormat, ...)
{
	VA_LIST vl;

	VA_START(vl, wszFormat);

	m_wss.Reset();
	m_wss.AppendFormatVA(wszFormat, vl);

	VA_END(vl);

	(* this) << m_wss.Wsz();
	return *this;
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write of CHAR with conversion
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator << 
	(
	const CHAR *sz
	)
{
	return AppendFormat(GPOS_WSZ_LIT("%s"), sz);
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a single WCHAR with conversion
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator << 
	(
	const WCHAR wc
	)
{
	return AppendFormat(GPOS_WSZ_LIT("%lc"), wc);
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a single CHAR with conversion
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator << 
	(
	const CHAR c
	)
{
	return AppendFormat(GPOS_WSZ_LIT("%c"), c);
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a ULONG with conversion
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator << 
	(
	ULONG ul
	)
{
	switch(FstreamMod())
	{
		case EsmDec:
			return AppendFormat(GPOS_WSZ_LIT("%u"), ul);
			
		case EsmHex:
			return AppendFormat(GPOS_WSZ_LIT("%x"), ul);

		default:
			GPOS_ASSERT(!"Unexpected stream mode");
	}

	return *this;
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a ULLONG with conversion
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator << 
	(
	ULLONG ull
	)
{
	switch(FstreamMod())
	{
		case EsmDec:
			return AppendFormat(GPOS_WSZ_LIT("%llu"), ull);
			
		case EsmHex:
			return AppendFormat(GPOS_WSZ_LIT("%llx"), ull);

		default:
			GPOS_ASSERT(!"Unexpected stream mode");
	}

	return *this;
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write an INT with conversion
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator << 
	(
	INT i
	)
{
	switch(FstreamMod())
	{
		case EsmDec:
			return AppendFormat(GPOS_WSZ_LIT("%d"), i);
			
		case EsmHex:
			return AppendFormat(GPOS_WSZ_LIT("%x"), i);

		default:
			GPOS_ASSERT(!"Unexpected stream mode");
	}

	return *this;
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a LINT with conversion
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator <<
	(
	LINT li
	)
{
	switch(FstreamMod())
	{
		case EsmDec:
			return AppendFormat(GPOS_WSZ_LIT("%lld"), li);

		case EsmHex:
			return AppendFormat(GPOS_WSZ_LIT("%llx"), li);

		default:
			GPOS_ASSERT(!"Unexpected stream mode");
	}

	return *this;
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a double-precision floating point number
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator <<
	(
	const DOUBLE d
	)
{
	return AppendFormat(GPOS_WSZ_LIT("%f"), d);
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		write a pointer with conversion
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator << 
	(
	const void *pv
	)
{
	return AppendFormat(GPOS_WSZ_LIT("%p"), pv);
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		Change the stream modifier
//
//---------------------------------------------------------------------------
IOstream &
COstream::operator << 
	(
	EstreamMod esm
	)
{
	m_esm = esm;
	return *this;
}

//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		Return the stream modifier
//
//---------------------------------------------------------------------------
IOstream::EstreamMod
COstream::FstreamMod
	(
	) const
{
	return m_esm;
}


//---------------------------------------------------------------------------
//	@function:
//		COstream::operator<<
//
//	@doc:
//		To support << std::endl
//
//---------------------------------------------------------------------------

IOstream &
COstream::operator << 
	(
	WOSTREAM& (*pfunc)(WOSTREAM&) __attribute__ ((unused))
	)
{
// This extra safety check is not portable accross different C++
// standard-library implementations that may implement std::endl as a template.
// It is enabled only for GNU libstdc++, where it is known to work.
#if defined(GPOS_DEBUG) && defined(__GLIBCXX__)
	typedef WOSTREAM& (*TManip)(WOSTREAM&);
	TManip tmf = pfunc;
	GPOS_ASSERT(tmf==static_cast<TManip>(std::endl) && "Only std::endl allowed");
#endif
	(* this) << '\n';
	return *this;
}

// EOF

