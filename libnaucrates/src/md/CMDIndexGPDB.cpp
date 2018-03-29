//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIndexGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing metadata indexes
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/IMDPartConstraint.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::CMDIndexGPDB
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CMDIndexGPDB::CMDIndexGPDB
	(
	IMemoryPool *pmp, 
	IMDId *pmdid, 
	CMDName *pmdname,
	BOOL fClustered, 
	IMDIndex::EmdindexType emdindt,
	IMDId *pmdidItemType,
	DrgPul *pdrgpulKeyCols,
	DrgPul *pdrgpulIncludedCols,
	DrgPmdid *pdrgpmdidOpClasses,
	IMDPartConstraint *pmdpartcnstr
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_fClustered(fClustered),
	m_emdindt(emdindt),
	m_pmdidItemType(pmdidItemType),
	m_pdrgpulKeyCols(pdrgpulKeyCols),
	m_pdrgpulIncludedCols(pdrgpulIncludedCols),
	m_pdrgpmdidOpClasses(pdrgpmdidOpClasses),
	m_pmdpartcnstr(pmdpartcnstr)
{
	GPOS_ASSERT(pmdid->FValid());
	GPOS_ASSERT(IMDIndex::EmdindSentinel > emdindt);
	GPOS_ASSERT(NULL != pdrgpulKeyCols);
	GPOS_ASSERT(0 < pdrgpulKeyCols->UlLength());
	GPOS_ASSERT(NULL != pdrgpulIncludedCols);
	GPOS_ASSERT_IMP(NULL != pmdidItemType, IMDIndex::EmdindBitmap == emdindt);
	GPOS_ASSERT_IMP(IMDIndex::EmdindBitmap == emdindt, NULL != pmdidItemType && pmdidItemType->FValid());
	GPOS_ASSERT(NULL != pdrgpmdidOpClasses);
	
	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::~CMDIndexGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIndexGPDB::~CMDIndexGPDB()
{
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pmdid->Release();
	CRefCount::SafeRelease(m_pmdidItemType);
	m_pdrgpulKeyCols->Release();
	m_pdrgpulIncludedCols->Release();
	m_pdrgpmdidOpClasses->Release();
	CRefCount::SafeRelease(m_pmdpartcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this index
//
//---------------------------------------------------------------------------
IMDId *
CMDIndexGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::Mdname
//
//	@doc:
//		Returns the name of this index
//
//---------------------------------------------------------------------------
CMDName
CMDIndexGPDB::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::FClustered
//
//	@doc:
//		Is the index clustered
//
//---------------------------------------------------------------------------
BOOL
CMDIndexGPDB::FClustered() const
{
	return m_fClustered;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::Emdindt
//
//	@doc:
//		Index type
//
//---------------------------------------------------------------------------
IMDIndex::EmdindexType
CMDIndexGPDB::Emdindt() const
{
	return m_emdindt;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::UlKeys
//
//	@doc:
//		Returns the number of index keys
//
//---------------------------------------------------------------------------
ULONG
CMDIndexGPDB::UlKeys() const
{
	return m_pdrgpulKeyCols->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::UlKey
//
//	@doc:
//		Returns the n-th key column
//
//---------------------------------------------------------------------------
ULONG
CMDIndexGPDB::UlKey
	(
	ULONG ulPos
	) 
	const
{
	return *((*m_pdrgpulKeyCols)[ulPos]);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::UlPosInKey
//
//	@doc:
//		Return the position of the key column in the index
//
//---------------------------------------------------------------------------
ULONG
CMDIndexGPDB::UlPosInKey
	(
	ULONG ulCol
	)
	const
{
	const ULONG ulSize = UlKeys();

	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		if (UlKey(ul) == ulCol)
		{
			return ul;
		}
	}

	return gpos::ulong_max;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::UlIncludedCols
//
//	@doc:
//		Returns the number of included columns
//
//---------------------------------------------------------------------------
ULONG
CMDIndexGPDB::UlIncludedCols() const
{
	return m_pdrgpulIncludedCols->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::UlIncludedCol
//
//	@doc:
//		Returns the n-th included column
//
//---------------------------------------------------------------------------
ULONG
CMDIndexGPDB::UlIncludedCol
	(
	ULONG ulPos
	)
	const
{
	return *((*m_pdrgpulIncludedCols)[ulPos]);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::UlPosInIncludedCol
//
//	@doc:
//		Return the position of the included column in the index
//
//---------------------------------------------------------------------------
ULONG
CMDIndexGPDB::UlPosInIncludedCol
	(
	ULONG ulCol
	)
	const
{
	const ULONG ulSize = UlIncludedCols();

	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		if (UlIncludedCol(ul) == ulCol)
		{
			return ul;
		}
	}

	GPOS_ASSERT("Column not found in Index's included columns");

	return gpos::ulong_max;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::Pmdpartcnstr
//
//	@doc:
//		Return the part constraint
//
//---------------------------------------------------------------------------
IMDPartConstraint *
CMDIndexGPDB::Pmdpartcnstr() const
{
	return m_pmdpartcnstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::Serialize
//
//	@doc:
//		Serialize MD index in DXL format
//
//---------------------------------------------------------------------------
void
CMDIndexGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenIndex));
	
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIndexClustered), m_fClustered);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIndexType), PstrIndexType(m_emdindt));
	if (NULL != m_pmdidItemType)
	{
		m_pmdidItemType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenIndexItemType));
	}
		
	// serialize index keys
	CWStringDynamic *pstrKeyCols = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulKeyCols);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIndexKeyCols), pstrKeyCols);
	GPOS_DELETE(pstrKeyCols);

	CWStringDynamic *pstrAvailCols = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulIncludedCols);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIndexIncludedCols), pstrAvailCols);
	GPOS_DELETE(pstrAvailCols);
		
	// serialize operator class information
	SerializeMDIdList(pxmlser, m_pdrgpmdidOpClasses, 
						CDXLTokens::PstrToken(EdxltokenOpClasses), 
						CDXLTokens::PstrToken(EdxltokenOpClass));
	
	if (NULL != m_pmdpartcnstr)
	{
		m_pmdpartcnstr->Serialize(pxmlser);
	}
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenIndex));
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::DebugPrint
//
//	@doc:
//		Prints a MD index to the provided output
//
//---------------------------------------------------------------------------
void
CMDIndexGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Index id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;
	
	os << "Index name: " << (Mdname()).Pstr()->Wsz() << std::endl;
	os << "Index type: " << PstrIndexType(m_emdindt)->Wsz() << std::endl;

	os << "Index keys: ";
	for (ULONG ul = 0; ul < UlKeys(); ul++)
	{
		ULONG ulKey = UlKey(ul);
		if (ul > 0)
		{
			os << ", ";
		}
		os << ulKey;
	}
	os << std::endl;	

	os << "Included Columns: ";
	for (ULONG ul = 0; ul < UlIncludedCols(); ul++)
	{
		ULONG ulKey = UlIncludedCol(ul);
		if (ul > 0)
		{
			os << ", ";
		}
		os << ulKey;
	}
	os << std::endl;
}

#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::PmdidType
//
//	@doc:
//		Type of items returned by the index
//
//---------------------------------------------------------------------------
IMDId *
CMDIndexGPDB::PmdidItemType() const
{
	return m_pmdidItemType;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIndexGPDB::FCompatible
//
//	@doc:
//		Check if given scalar comparison can be used with the index key 
// 		at the specified position
//
//---------------------------------------------------------------------------
BOOL
CMDIndexGPDB::FCompatible
	(
	const IMDScalarOp *pmdscop, 
	ULONG ulKeyPos
	)
	const
{
	GPOS_ASSERT(NULL != pmdscop);
	GPOS_ASSERT(ulKeyPos < m_pdrgpmdidOpClasses->UlLength());
	
	// check if the index opclass for the key at the given position is one of 
	// the classes the scalar comparison belongs to
	const IMDId *pmdidOpClass = (*m_pdrgpmdidOpClasses)[ulKeyPos];
	
	const ULONG ulScOpClasses = pmdscop->UlOpCLasses();
	
	for (ULONG ul = 0; ul < ulScOpClasses; ul++)
	{
		if (pmdidOpClass->FEquals(pmdscop->PmdidOpClass(ul)))
		{
			return true;
		}
	}
	
	return false;
}

// EOF

