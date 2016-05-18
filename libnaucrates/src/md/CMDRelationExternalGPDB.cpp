//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDRelationExternalGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing MD cache external relations
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/md/CMDRelationExternalGPDB.h"
#include "naucrates/md/CMDUtilsGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::CMDRelationExternalGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDRelationExternalGPDB::CMDRelationExternalGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	Ereldistrpolicy ereldistrpolicy,
	DrgPmdcol *pdrgpmdcol,
	DrgPul *pdrgpulDistrColumns,
	BOOL fConvertHashToRandom,
	DrgPdrgPul *pdrgpdrgpulKeys,
	DrgPmdid *pdrgpmdidIndices,
	DrgPmdid *pdrgpmdidTriggers,
 	DrgPmdid *pdrgpmdidCheckConstraint,
	INT iRejectLimit,
	BOOL fRejLimitInRows,
	IMDId *pmdidFmtErrRel
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_ereldistrpolicy(ereldistrpolicy),
	m_pdrgpmdcol(pdrgpmdcol),
	m_ulDroppedCols(0),
	m_pdrgpulDistrColumns(pdrgpulDistrColumns),
	m_fConvertHashToRandom(fConvertHashToRandom),
	m_pdrgpdrgpulKeys(pdrgpdrgpulKeys),
	m_pdrgpmdidIndices(pdrgpmdidIndices),
	m_pdrgpmdidTriggers(pdrgpmdidTriggers),
	m_pdrgpmdidCheckConstraint(pdrgpmdidCheckConstraint),
	m_iRejectLimit(iRejectLimit),
	m_fRejLimitInRows(fRejLimitInRows),
	m_pmdidFmtErrRel(pmdidFmtErrRel),
	m_phmululNonDroppedCols(NULL),
	m_phmiulAttno2Pos(NULL),
	m_pdrgpulNonDroppedCols(NULL)
{
	GPOS_ASSERT(pmdid->FValid());
	GPOS_ASSERT(NULL != pdrgpmdcol);
	GPOS_ASSERT(NULL != pdrgpmdidIndices);
	GPOS_ASSERT(NULL != pdrgpmdidTriggers);
	GPOS_ASSERT(NULL != pdrgpmdidCheckConstraint);
	GPOS_ASSERT_IMP(fConvertHashToRandom,
				IMDRelation::EreldistrHash == ereldistrpolicy &&
				"Converting hash distributed table to random only possible for hash distributed tables");

	m_phmululNonDroppedCols = GPOS_NEW(m_pmp) HMUlUl(m_pmp);
	m_phmiulAttno2Pos = GPOS_NEW(m_pmp) HMIUl(m_pmp);
	m_pdrgpulNonDroppedCols = GPOS_NEW(m_pmp) DrgPul(m_pmp);
	
	CMDUtilsGPDB::InitializeMDColInfo(pmp, pdrgpmdcol, m_phmiulAttno2Pos, m_pdrgpulNonDroppedCols, m_phmululNonDroppedCols);
	m_ulDroppedCols = m_pdrgpulNonDroppedCols->UlLength();

	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::~CMDRelationExternalGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDRelationExternalGPDB::~CMDRelationExternalGPDB()
{
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pmdid->Release();
	m_pdrgpmdcol->Release();
	CRefCount::SafeRelease(m_pdrgpulDistrColumns);
	CRefCount::SafeRelease(m_pdrgpdrgpulKeys);
	m_pdrgpmdidIndices->Release();
	m_pdrgpmdidTriggers->Release();
	m_pdrgpmdidCheckConstraint->Release();
	CRefCount::SafeRelease(m_pmdidFmtErrRel);

	CRefCount::SafeRelease(m_phmululNonDroppedCols);
	CRefCount::SafeRelease(m_phmiulAttno2Pos);
	CRefCount::SafeRelease(m_pdrgpulNonDroppedCols);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this relation
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationExternalGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::Mdname
//
//	@doc:
//		Returns the name of this relation
//
//---------------------------------------------------------------------------
CMDName
CMDRelationExternalGPDB::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::Ereldistribution
//
//	@doc:
//		Returns the distribution policy for this relation
//
//---------------------------------------------------------------------------
IMDRelation::Ereldistrpolicy
CMDRelationExternalGPDB::Ereldistribution() const
{
	return m_ereldistrpolicy;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlColumns
//
//	@doc:
//		Returns the number of columns of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlColumns() const
{
	GPOS_ASSERT(NULL != m_pdrgpmdcol);

	return m_pdrgpmdcol->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::FHasDroppedColumns
//
//	@doc:
//		Does relation have dropped columns
//
//---------------------------------------------------------------------------
BOOL
CMDRelationExternalGPDB::FHasDroppedColumns() const
{	
	return 0 < m_ulDroppedCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlNonDroppedCols
//
//	@doc:
//		Number of non-dropped columns
//
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlNonDroppedCols() const
{	
	return UlColumns() - m_ulDroppedCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::PdrgpulNonDroppedCols
//
//	@doc:
//		Returns the original positions of all the non-dropped columns
//
//---------------------------------------------------------------------------
DrgPul *
CMDRelationExternalGPDB::PdrgpulNonDroppedCols() const
{
	return m_pdrgpulNonDroppedCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlPosNonDropped
//
//	@doc:
//		Return the absolute position of the given attribute position excluding 
//		dropped columns
//
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlPosNonDropped
	(
	ULONG ulPos
	)
	const
{	
	GPOS_ASSERT(ulPos <= UlColumns());
	
	if (!FHasDroppedColumns())
	{
		return ulPos;
	}
	
	ULONG *pul = m_phmululNonDroppedCols->PtLookup(&ulPos);
	
	GPOS_ASSERT(NULL != pul);
	return *pul;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlPosFromAttno
//
//	@doc:
//		Return the position of a column in the metadata object given the
//		attribute number in the system catalog
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlPosFromAttno
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
//		CMDRelationExternalGPDB::FConvertHashToRandom
//
//	@doc:
//		Return true if a hash distributed table needs to be considered as random during planning
//---------------------------------------------------------------------------
BOOL
CMDRelationExternalGPDB::FConvertHashToRandom() const
{
	return m_fConvertHashToRandom;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::IRejectLimit
//
//	@doc:
//		Reject limit
//
//---------------------------------------------------------------------------
INT
CMDRelationExternalGPDB::IRejectLimit() const
{
	return m_iRejectLimit;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::FRejLimitInRows
//
//	@doc:
//		Is the reject limit in rows?
//
//---------------------------------------------------------------------------
BOOL
CMDRelationExternalGPDB::FRejLimitInRows() const
{
	return m_fRejLimitInRows;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::PmdidFmtErrRel
//
//	@doc:
//		Format error table mdid
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationExternalGPDB::PmdidFmtErrRel() const
{
	return m_pmdidFmtErrRel;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlKeySets
//
//	@doc:
//		Returns the number of key sets
//
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlKeySets() const
{
	return m_pdrgpdrgpulKeys->UlSafeLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::PdrgpulKeyset
//
//	@doc:
//		Returns the key set at the specified position
//
//---------------------------------------------------------------------------
const DrgPul *
CMDRelationExternalGPDB::PdrgpulKeyset
	(
	ULONG ulPos
	)
	const
{
	GPOS_ASSERT(NULL != m_pdrgpdrgpulKeys);

	return (*m_pdrgpdrgpulKeys)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlDistrColumns
//
//	@doc:
//		Returns the number of columns in the distribution column list of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlDistrColumns() const
{
	return m_pdrgpulDistrColumns->UlSafeLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlIndices
//
//	@doc:
//		Returns the number of indices of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlIndices() const
{
	return m_pdrgpmdidIndices->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlTriggers
//
//	@doc:
//		Returns the number of triggers of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlTriggers() const
{
	return m_pdrgpmdidTriggers->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::Pmdcol
//
//	@doc:
//		Returns the column at the specified position
//
//---------------------------------------------------------------------------
const IMDColumn *
CMDRelationExternalGPDB::Pmdcol
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
//		CMDRelationExternalGPDB::PmdcolDistrColumn
//
//	@doc:
//		Returns the distribution column at the specified position in the distribution column list
//
//---------------------------------------------------------------------------
const IMDColumn *
CMDRelationExternalGPDB::PmdcolDistrColumn
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
//		CMDRelationExternalGPDB::PmdidIndex
//
//	@doc:
//		Returns the id of the index at the specified position of the index array
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationExternalGPDB::PmdidIndex
	(
	ULONG ulPos
	)
	const
{
	return (*m_pdrgpmdidIndices)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::PmdidTrigger
//
//	@doc:
//		Returns the id of the trigger at the specified position of the trigger array
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationExternalGPDB::PmdidTrigger
	(
	ULONG ulPos
	)
	const
{
	return (*m_pdrgpmdidTriggers)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::UlCheckConstraints
//
//	@doc:
//		Returns the number of check constraints on this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationExternalGPDB::UlCheckConstraints() const
{
	return m_pdrgpmdidCheckConstraint->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::PmdidCheckConstraint
//
//	@doc:
//		Returns the id of the check constraint at the specified position of
//		the check constraint array
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationExternalGPDB::PmdidCheckConstraint
	(
	ULONG ulPos
	)
	const
{
	return (*m_pdrgpmdidCheckConstraint)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDRelationExternalGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenRelationExternal));

	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelDistrPolicy), PstrDistrPolicy(m_ereldistrpolicy));

	if (EreldistrHash == m_ereldistrpolicy)
	{
		GPOS_ASSERT(NULL != m_pdrgpulDistrColumns);

		// serialize distribution columns
		CWStringDynamic *pstrDistrColumns = PstrColumns(m_pmp, m_pdrgpulDistrColumns);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDistrColumns), pstrDistrColumns);
		GPOS_DELETE(pstrDistrColumns);
	}

	// serialize key sets
	const ULONG ulKeySets = m_pdrgpdrgpulKeys->UlSafeLength();
	if (0 < ulKeySets)
	{
		CWStringDynamic *pstrKeys = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpdrgpulKeys);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenKeys), pstrKeys);
		GPOS_DELETE(pstrKeys);
	}

	if (0 <= m_iRejectLimit)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenExtRelRejLimit), m_iRejectLimit);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenExtRelRejLimitInRows), m_fRejLimitInRows);
	}

	if (NULL != m_pmdidFmtErrRel)
	{
		m_pmdidFmtErrRel->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenExtRelFmtErrRel));
	}

	if (m_fConvertHashToRandom)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenConvertHashToRandom), m_fConvertHashToRandom);
	}

	// serialize columns
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenColumns));
	for (ULONG ul = 0; ul < m_pdrgpmdcol->UlLength(); ul++)
	{
		IMDColumn *pmdcol = (*m_pdrgpmdcol)[ul];
		pmdcol->Serialize(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenColumns));

	// serialize index information
	SerializeMDIdList(pxmlser, m_pdrgpmdidIndices,
						CDXLTokens::PstrToken(EdxltokenIndexes),
						CDXLTokens::PstrToken(EdxltokenIndex));

	// serialize trigger information
	SerializeMDIdList(pxmlser, m_pdrgpmdidTriggers,
						CDXLTokens::PstrToken(EdxltokenTriggers),
						CDXLTokens::PstrToken(EdxltokenTrigger));

	// serialize check constraint information
	SerializeMDIdList(pxmlser, m_pdrgpmdidCheckConstraint,
						CDXLTokens::PstrToken(EdxltokenCheckConstraints),
						CDXLTokens::PstrToken(EdxltokenCheckConstraint));

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenRelationExternal));
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDRelationExternalGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDRelationExternalGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "External Relation id: ";
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

	os << "Indexes: ";
	CDXLUtils::DebugPrintDrgpmdid(os, m_pdrgpmdidIndices);

	os << "Triggers: ";
	CDXLUtils::DebugPrintDrgpmdid(os, m_pdrgpmdidTriggers);

	os << "Check Constraint: ";
	CDXLUtils::DebugPrintDrgpmdid(os, m_pdrgpmdidCheckConstraint);

	os << "Reject limit: " << m_iRejectLimit;
	if (m_fRejLimitInRows)
	{
		os << " Rows" << std::endl;
	}
	else
	{
		os << " Percent" << std::endl;
	}

	if (NULL != PmdidFmtErrRel())
	{
		os << "Format Error Table: ";
		PmdidFmtErrRel()->OsPrint(os);
		os << std::endl;
	}
}

#endif // GPOS_DEBUG

// EOF

