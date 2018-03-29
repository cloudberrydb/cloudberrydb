//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDColumn.cpp
//
//	@doc:
//		Implementation of the class for representing metadata about relation's
//		columns
//---------------------------------------------------------------------------


#include "naucrates/md/CMDColumn.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::CMDColumn
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDColumn::CMDColumn
	(
	CMDName *pmdname,
	INT iAttNo,
	IMDId *pmdidType,
	INT iTypeModifier,
	BOOL fNullable,
	BOOL fDropped,
	CDXLNode *pdxnlDefaultValue,
	ULONG ulLength
	)
	:
	m_pmdname(pmdname),
	m_iAttNo(iAttNo),
	m_pmdidType(pmdidType),
	m_iTypeModifier(iTypeModifier),
	m_fNullable(fNullable),
	m_fDropped(fDropped),
	m_ulLength(ulLength),
	m_pdxlnDefaultValue(pdxnlDefaultValue)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::~CMDColumn
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDColumn::~CMDColumn()
{
	GPOS_DELETE(m_pmdname);
	m_pmdidType->Release();
	CRefCount::SafeRelease(m_pdxlnDefaultValue);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::Mdname
//
//	@doc:
//		Returns the column name
//
//---------------------------------------------------------------------------
CMDName
CMDColumn::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::IAttno
//
//	@doc:
//		Attribute number
//
//---------------------------------------------------------------------------
INT
CMDColumn::IAttno() const
{
	return m_iAttNo;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::PmdidType
//
//	@doc:
//		Attribute type id
//
//---------------------------------------------------------------------------
IMDId *
CMDColumn::PmdidType() const
{
	return m_pmdidType;
}

INT
CMDColumn::ITypeModifier() const
{
	return m_iTypeModifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::FNullable
//
//	@doc:
//		Returns whether NULLs are allowed for this column
//
//---------------------------------------------------------------------------
BOOL
CMDColumn::FNullable() const
{
	return m_fNullable;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::FDropped
//
//	@doc:
//		Returns whether column is dropped
//
//---------------------------------------------------------------------------
BOOL
CMDColumn::FDropped() const
{
	return m_fDropped;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::Serialize
//
//	@doc:
//		Serialize column metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDColumn::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumn));
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenAttno), m_iAttNo);

	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	if (IDefaultTypeModifier != ITypeModifier())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), ITypeModifier());
	}

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColumnNullable), m_fNullable);
	if (gpos::ulong_max != m_ulLength)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColWidth), m_ulLength);
	}

	if (m_fDropped)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColDropped), m_fDropped);
	}
	
	// serialize default value
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumnDefaultValue));
	
	if (NULL != m_pdxlnDefaultValue)
	{
		m_pdxlnDefaultValue->SerializeToDXL(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumnDefaultValue));
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumn));
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDColumn::DebugPrint
//
//	@doc:
//		Debug print of the column in the provided stream
//
//---------------------------------------------------------------------------
void
CMDColumn::DebugPrint
	(
	IOstream &os
	) 
	const
{
	os << "Attno: " << IAttno() << std::endl;
	
	os << "Column name: " << (Mdname()).Pstr()->Wsz() << std::endl;
	os << "Column type: ";
	PmdidType()->OsPrint(os);
	os << std::endl;

	const CWStringConst *pstrNullsAllowed = FNullable() ?
												CDXLTokens::PstrToken(EdxltokenTrue) :
												CDXLTokens::PstrToken(EdxltokenFalse);
	
	os << "Nulls allowed: " << pstrNullsAllowed->Wsz() << std::endl;
}

#endif // GPOS_DEBUG

// EOF

