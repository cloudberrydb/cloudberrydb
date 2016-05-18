//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CMDRelationCtasGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing MD cache CTAS relations
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/md/CMDRelationCtasGPDB.h"
#include "naucrates/md/CMDUtilsGPDB.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/operators/CDXLCtasStorageOptions.h"

#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::CMDRelationCtasGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDRelationCtasGPDB::CMDRelationCtasGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdnameSchema,
	CMDName *pmdname,
	BOOL fTemporary,
	BOOL fHasOids,
	Erelstoragetype erelstorage,
	Ereldistrpolicy ereldistrpolicy,
	DrgPmdcol *pdrgpmdcol,
	DrgPul *pdrgpulDistrColumns,
	DrgPdrgPul *pdrgpdrgpulKeys,
	CDXLCtasStorageOptions *pdxlctasopt,
	DrgPi *pdrgpiVarTypeMod
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdnameSchema(pmdnameSchema),
	m_pmdname(pmdname),
	m_fTemporary(fTemporary),
	m_fHasOids(fHasOids),
	m_erelstorage(erelstorage),
	m_ereldistrpolicy(ereldistrpolicy),
	m_pdrgpmdcol(pdrgpmdcol),
	m_pdrgpulDistrColumns(pdrgpulDistrColumns),
	m_pdrgpdrgpulKeys(pdrgpdrgpulKeys),
	m_pdrgpulNonDroppedCols(NULL),
	m_pdxlctasopt(pdxlctasopt),
	m_pdrgpiVarTypeMod(pdrgpiVarTypeMod)
{
	GPOS_ASSERT(pmdid->FValid());
	GPOS_ASSERT(NULL != pdrgpmdcol);
	GPOS_ASSERT(NULL != pdxlctasopt);
	GPOS_ASSERT(IMDRelation::ErelstorageSentinel > m_erelstorage);	
	GPOS_ASSERT(0 == pdrgpdrgpulKeys->UlLength());
	GPOS_ASSERT(NULL != pdrgpiVarTypeMod);
	
	m_phmiulAttno2Pos = GPOS_NEW(m_pmp) HMIUl(m_pmp);
	m_pdrgpulNonDroppedCols = GPOS_NEW(m_pmp) DrgPul(m_pmp);
	
	CMDUtilsGPDB::InitializeMDColInfo(pmp, pdrgpmdcol, m_phmiulAttno2Pos, m_pdrgpulNonDroppedCols, NULL /* m_phmululNonDroppedCols */);

	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::~CMDRelationCtasGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDRelationCtasGPDB::~CMDRelationCtasGPDB()
{
	GPOS_DELETE(m_pmdnameSchema);
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pmdid->Release();
	m_pdrgpmdcol->Release();
	m_pdrgpdrgpulKeys->Release();
	CRefCount::SafeRelease(m_pdrgpulDistrColumns);
	CRefCount::SafeRelease(m_phmiulAttno2Pos);
	CRefCount::SafeRelease(m_pdrgpulNonDroppedCols);
	m_pdxlctasopt->Release();
	m_pdrgpiVarTypeMod->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this relation
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationCtasGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::Mdname
//
//	@doc:
//		Returns the name of this relation
//
//---------------------------------------------------------------------------
CMDName
CMDRelationCtasGPDB::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::PmdnameSchema
//
//	@doc:
//		Returns schema name
//
//---------------------------------------------------------------------------
CMDName *
CMDRelationCtasGPDB::PmdnameSchema() const
{
	return m_pmdnameSchema;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::Ereldistribution
//
//	@doc:
//		Returns the distribution policy for this relation
//
//---------------------------------------------------------------------------
IMDRelation::Ereldistrpolicy
CMDRelationCtasGPDB::Ereldistribution() const
{
	return m_ereldistrpolicy;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::UlColumns
//
//	@doc:
//		Returns the number of columns of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationCtasGPDB::UlColumns() const
{
	GPOS_ASSERT(NULL != m_pdrgpmdcol);

	return m_pdrgpmdcol->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::UlPosFromAttno
//
//	@doc:
//		Return the position of a column in the metadata object given the
//		attribute number in the system catalog
//---------------------------------------------------------------------------
ULONG
CMDRelationCtasGPDB::UlPosFromAttno
	(
	INT iAttno
	)
	const
{
	ULONG *pul = m_phmiulAttno2Pos->PtLookup(&iAttno);
	GPOS_ASSERT(NULL != pul);

	return *pul;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::UlDistrColumns
//
//	@doc:
//		Returns the number of columns in the distribution column list of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationCtasGPDB::UlDistrColumns() const
{
	return m_pdrgpulDistrColumns->UlSafeLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::Pmdcol
//
//	@doc:
//		Returns the column at the specified position
//
//---------------------------------------------------------------------------
const IMDColumn *
CMDRelationCtasGPDB::Pmdcol
	(
	ULONG ulPos
	)
	const
{
	GPOS_ASSERT(ulPos < m_pdrgpmdcol->UlLength());

	return (*m_pdrgpmdcol)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::PmdcolDistrColumn
//
//	@doc:
//		Returns the distribution column at the specified position in the distribution column list
//
//---------------------------------------------------------------------------
const IMDColumn *
CMDRelationCtasGPDB::PmdcolDistrColumn
	(
	ULONG ulPos
	)
	const
{
	GPOS_ASSERT(ulPos < m_pdrgpulDistrColumns->UlLength());

	ULONG ulDistrKeyPos = (*(*m_pdrgpulDistrColumns)[ulPos]);
	return Pmdcol(ulDistrKeyPos);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDRelationCtasGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenRelationCTAS));

	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	if (NULL != m_pmdnameSchema)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSchema), m_pmdnameSchema->Pstr());
	}
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelTemporary), m_fTemporary);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelHasOids), m_fHasOids);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelStorageType), IMDRelation::PstrStorageType(m_erelstorage));

	// serialize vartypmod list
	CWStringDynamic *pstrVarTypeModList = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpiVarTypeMod);
	GPOS_ASSERT(NULL != pstrVarTypeModList);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenVarTypeModList), pstrVarTypeModList);
	GPOS_DELETE(pstrVarTypeModList);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelDistrPolicy), PstrDistrPolicy(m_ereldistrpolicy));

	if (EreldistrHash == m_ereldistrpolicy)
	{
		GPOS_ASSERT(NULL != m_pdrgpulDistrColumns);

		// serialize distribution columns
		CWStringDynamic *pstrDistrColumns = PstrColumns(m_pmp, m_pdrgpulDistrColumns);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDistrColumns), pstrDistrColumns);
		GPOS_DELETE(pstrDistrColumns);
	}

	// serialize columns
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenColumns));
	const ULONG ulCols = m_pdrgpmdcol->UlLength();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		IMDColumn *pmdcol = (*m_pdrgpmdcol)[ul];
		pmdcol->Serialize(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenColumns));

	m_pdxlctasopt->Serialize(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenRelationCTAS));
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDRelationCtasGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDRelationCtasGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "CTAS Relation id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;

	os << "Relation name: " << (Mdname()).Pstr()->Wsz() << std::endl;

	os << "Distribution policy: " << PstrDistrPolicy(m_ereldistrpolicy)->Wsz() << std::endl;

	os << "Relation columns: " << std::endl;
	const ULONG ulColumns = UlColumns();
	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		const IMDColumn *pimdcol = Pmdcol(ul);
		pimdcol->DebugPrint(os);
	}
	os << std::endl;

	os << "Distributed by: ";
	const ULONG ulDistrColumns = UlDistrColumns();
	for (ULONG ul = 0; ul < ulDistrColumns; ul++)
	{
		if (0 < ul)
		{
			os << ", ";
		}

		const IMDColumn *pimdcolDistrKey = PmdcolDistrColumn(ul);
		os << (pimdcolDistrKey->Mdname()).Pstr()->Wsz();
	}

	os << std::endl;
}

#endif // GPOS_DEBUG

// EOF

