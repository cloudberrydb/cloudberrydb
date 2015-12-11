//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CXMLSerializer.cpp
//
//	@doc:
//		Implementation of the class for creating XML documents.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;

#define GPDXL_SERIALIZE_CFA_FREQUENCY 30

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::~CXMLSerializer
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CXMLSerializer::~CXMLSerializer()
{
	GPOS_DELETE(m_strstackElems);
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::StartDocument
//
//	@doc:
//		Write the opening tags for the XML document
//
//---------------------------------------------------------------------------
void
CXMLSerializer::StartDocument()
{
	GPOS_ASSERT(m_strstackElems->FEmpty());
	m_os << CDXLTokens::PstrToken(EdxltokenXMLDocHeader)->Wsz();
	if (m_fIndent)
	{
		m_os << std::endl;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::OpenElement
//
//	@doc:
//		Write an opening tag for the specified element
//
//---------------------------------------------------------------------------
void
CXMLSerializer::OpenElement
	(
	const CWStringBase *pstrNamespace,
	const CWStringBase *pstrElem
	)
{
	GPOS_ASSERT(NULL != pstrElem);
	
	m_ulIterLastCFA++;
	
	if (GPDXL_SERIALIZE_CFA_FREQUENCY < m_ulIterLastCFA)
	{
		GPOS_CHECK_ABORT;
		m_ulIterLastCFA = 0;
	}
	
	// put element on the stack
	m_strstackElems->Push(pstrElem);
	
	// write the closing bracket for the previous element if necessary and add indentation
	if (m_fOpenTag)
	{
		m_os << CDXLTokens::PstrToken(EdxltokenBracketCloseTag)->Wsz(); // >
		if (m_fIndent)
		{
			m_os << std::endl;
		}
	}
	
	Indent();
	
	// write element to stream
	m_os << CDXLTokens::PstrToken(EdxltokenBracketOpenTag)->Wsz();			// <
	
	if(NULL != pstrNamespace)
	{
		m_os << pstrNamespace->Wsz() << CDXLTokens::PstrToken(EdxltokenColon)->Wsz();	// "namespace:"
	}
	m_os << pstrElem->Wsz();
	
	m_fOpenTag = true;
	m_ulLevel++;
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::CloseElement
//
//	@doc:
//		Write a closing tag for the specified element
//
//---------------------------------------------------------------------------
void
CXMLSerializer::CloseElement
	(
	const CWStringBase *pstrNamespace,
	const CWStringBase *pstrElem
	)
{
	GPOS_ASSERT(NULL != pstrElem);
	GPOS_ASSERT(0 < m_ulLevel);
	
	m_ulLevel--;
	
	// assert element is on top of the stack
#ifdef GPOS_DEBUG
	const CWStringBase *strOpenElem = 
#endif
	m_strstackElems->Pop();
	
	GPOS_ASSERT(strOpenElem->FEquals(pstrElem));
	
	if (m_fOpenTag)
	{
		// singleton element with no children - close the element with "/>"
		m_os << CDXLTokens::PstrToken(EdxltokenBracketCloseSingletonTag)->Wsz();	// />
		if (m_fIndent)
		{
			m_os << std::endl;
		}
		m_fOpenTag = false;
	}
	else
	{
		// add indentation
		Indent();
		
		// write closing tag for element to stream
		m_os << CDXLTokens::PstrToken(EdxltokenBracketOpenEndTag)->Wsz();		// </
		if(NULL != pstrNamespace)
		{
			m_os << pstrNamespace->Wsz() << CDXLTokens::PstrToken(EdxltokenColon)->Wsz();	// "namespace:"
		}
		m_os << pstrElem->Wsz() << CDXLTokens::PstrToken(EdxltokenBracketCloseTag)->Wsz(); // >
		if (m_fIndent)
		{
			m_os << std::endl;
		}
	}

	GPOS_CHECK_ABORT;
}


//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	const CWStringBase *pstrValue
	)
{
	GPOS_ASSERT(NULL != pstrAttr);
	GPOS_ASSERT(NULL != pstrValue);

	GPOS_ASSERT(m_fOpenTag);
	m_os << CDXLTokens::PstrToken(EdxltokenSpace)->Wsz()
		 << pstrAttr->Wsz()
		 << CDXLTokens::PstrToken(EdxltokenEq)->Wsz()		// = 
		 <<  CDXLTokens::PstrToken(EdxltokenQuote)->Wsz();	// "
	WriteEscaped(m_os, pstrValue);
	m_os << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz();	// "
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	const CHAR *szValue
	)
{
	GPOS_ASSERT(NULL != pstrAttr);
	GPOS_ASSERT(NULL != szValue);

	GPOS_ASSERT(m_fOpenTag);
	m_os << CDXLTokens::PstrToken(EdxltokenSpace)->Wsz()
		 << pstrAttr->Wsz()
		 << CDXLTokens::PstrToken(EdxltokenEq)->Wsz()		// = 
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz()	// "
		 << szValue
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz();	// "
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag.
//		Same as above but with a ULONG value
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	ULONG ulValue
	)
{
	GPOS_ASSERT(NULL != pstrAttr);

	GPOS_ASSERT(m_fOpenTag);
	m_os << CDXLTokens::PstrToken(EdxltokenSpace)->Wsz()
		 << pstrAttr->Wsz()
		 << CDXLTokens::PstrToken(EdxltokenEq)->Wsz()		// = 
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz()	// \"
		 << ulValue
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz();	// \"
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag.
//		Same as above but with a ULLONG value
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	ULLONG ullValue
	)
{
	GPOS_ASSERT(NULL != pstrAttr);

	GPOS_ASSERT(m_fOpenTag);
	m_os << CDXLTokens::PstrToken(EdxltokenSpace)->Wsz()
		 << pstrAttr->Wsz()
		 << CDXLTokens::PstrToken(EdxltokenEq)->Wsz()		// =
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz()	// \"
		 << ullValue
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz();	// \"
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag.
//		Same as above but with an INT value
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	INT iValue
	)
{
	GPOS_ASSERT(NULL != pstrAttr);

	GPOS_ASSERT(m_fOpenTag);
	m_os << CDXLTokens::PstrToken(EdxltokenSpace)->Wsz()
		 << pstrAttr->Wsz()
		 << CDXLTokens::PstrToken(EdxltokenEq)->Wsz()		// = 
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz()	// \"
		 << iValue
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz();	// \"
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag.
//		Same as above but with an LINT value
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	LINT lValue
	)
{
	GPOS_ASSERT(NULL != pstrAttr);

	GPOS_ASSERT(m_fOpenTag);
	m_os << CDXLTokens::PstrToken(EdxltokenSpace)->Wsz()
		 << pstrAttr->Wsz()
		 << CDXLTokens::PstrToken(EdxltokenEq)->Wsz()		// =
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz()	// \"
		 << lValue
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz();	// \"
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag.
//		Same as above but with a CDouble value
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	CDouble dValue
	)
{
	GPOS_ASSERT(NULL != pstrAttr);

	GPOS_ASSERT(m_fOpenTag);
	m_os << CDXLTokens::PstrToken(EdxltokenSpace)->Wsz()
		 << pstrAttr->Wsz()
		 << CDXLTokens::PstrToken(EdxltokenEq)->Wsz()		// = 
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz()	// \"
		 << dValue
		 << CDXLTokens::PstrToken(EdxltokenQuote)->Wsz();	// \"
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag.
//		Same as above but with a BOOL value
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	BOOL fValue
	)
{
	const CWStringConst *pstrValue = NULL;
	
	if (fValue)
	{
		pstrValue = CDXLTokens::PstrToken(EdxltokenTrue);
	}
	else
	{
		pstrValue = CDXLTokens::PstrToken(EdxltokenFalse);
	}

	AddAttribute(pstrAttr, pstrValue);
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::Indent
//
//	@doc:
//		Adds indentation to the output document according to the current nesting
//		level.
//
//---------------------------------------------------------------------------
void
CXMLSerializer::Indent()
{
	if (!m_fIndent)
	{
		return;
	}
	
	for (ULONG ul = 0; ul < m_ulLevel; ul++)
	{
		m_os << CDXLTokens::PstrToken(EdxltokenIndent)->Wsz();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::WriteEscaped
//
//	@doc:
//		Write the given string to the output stream by escaping it first
//
//---------------------------------------------------------------------------
void
CXMLSerializer::WriteEscaped
	(
	IOstream &os,
	const CWStringBase *pstr
	)
{
	GPOS_ASSERT(NULL != pstr);
	
	const ULONG ulSpecialChars = 8;
	
	const WCHAR *wszSpecialChars[][ulSpecialChars] = 
	{
	{GPOS_WSZ_LIT("\""), GPOS_WSZ_LIT("&quot;")},
	{GPOS_WSZ_LIT("'"), GPOS_WSZ_LIT("&apos;")},
	{GPOS_WSZ_LIT("<"), GPOS_WSZ_LIT("&lt;")},
	{GPOS_WSZ_LIT(">"), GPOS_WSZ_LIT("&gt;")},
	{GPOS_WSZ_LIT("&"), GPOS_WSZ_LIT("&amp;")},
	{GPOS_WSZ_LIT("\t"), GPOS_WSZ_LIT("&#x9;")},
	{GPOS_WSZ_LIT("\n"), GPOS_WSZ_LIT("&#xA;")},
	{GPOS_WSZ_LIT("\r"), GPOS_WSZ_LIT("&#xD;")}
	};
	
	const ULONG ulLength = pstr->UlLength();
	const WCHAR *wsz = pstr->Wsz();
	
	for (ULONG ulA = 0; ulA < ulLength; ulA++)
	{
		const WCHAR wc = wsz[ulA];
		
		const WCHAR *wszEscaped = NULL;
		
		for (ULONG ulB = 0; ulB < ulSpecialChars; ulB++)
		{
			if (0 == clib::IWcsNCmp(&wc, wszSpecialChars[ulB][0], 1 /*ulNum*/))
			{
				wszEscaped = wszSpecialChars[ulB][1];
				break;
			}
		}
		
		if (NULL != wszEscaped)
		{
			os << wszEscaped;
		}
		else
		{
			os << wc;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXMLSerializer::AddAttribute
//
//	@doc:
//		Adds an attribute-value pair to the currently open XML tag.
//		Same as above but with an LINT value
//
//---------------------------------------------------------------------------
void
CXMLSerializer::AddAttribute
	(
	const CWStringBase *pstrAttr,
	BOOL fNull,
	const BYTE *pba,
	ULONG ulLen
	)
{
	if (!fNull)
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromByteArray(m_pmp, pba, ulLen);
		AddAttribute(pstrAttr, pstr);
		GPOS_DELETE(pstr);
	}
}

// EOF
