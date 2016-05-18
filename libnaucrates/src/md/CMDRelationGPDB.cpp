//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDRelationGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing metadata cache relations
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDUtilsGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::CMDRelationGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDRelationGPDB::CMDRelationGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	BOOL fTemporary,
	Erelstoragetype erelstorage,
	Ereldistrpolicy ereldistrpolicy,
	DrgPmdcol *pdrgpmdcol,
	DrgPul *pdrgpulDistrColumns,
	DrgPul *pdrgpulPartColumns,
	ULONG ulPartitions,
	BOOL fConvertHashToRandom,
	DrgPdrgPul *pdrgpdrgpulKeys,
	DrgPmdid *pdrgpmdidIndices,
	DrgPmdid *pdrgpmdidTriggers,
 	DrgPmdid *pdrgpmdidCheckConstraint,
 	IMDPartConstraint *pmdpartcnstr,
 	BOOL fHasOids
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_fTemporary(fTemporary),
	m_erelstorage(erelstorage),
	m_ereldistrpolicy(ereldistrpolicy),
	m_pdrgpmdcol(pdrgpmdcol),
	m_ulDroppedCols(0),
	m_pdrgpulDistrColumns(pdrgpulDistrColumns),
	m_fConvertHashToRandom(fConvertHashToRandom),
	m_pdrgpulPartColumns(pdrgpulPartColumns),
	m_ulPartitions(ulPartitions),
	m_pdrgpdrgpulKeys(pdrgpdrgpulKeys),
	m_pdrgpmdidIndices(pdrgpmdidIndices),
	m_pdrgpmdidTriggers(pdrgpmdidTriggers),
	m_pdrgpmdidCheckConstraint(pdrgpmdidCheckConstraint),
	m_pmdpartcnstr(pmdpartcnstr),
	m_fHasOids(fHasOids),
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
//		CMDRelationGPDB::~CMDRelationGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDRelationGPDB::~CMDRelationGPDB()
{
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pmdid->Release();
	m_pdrgpmdcol->Release();
	CRefCount::SafeRelease(m_pdrgpulDistrColumns);
	CRefCount::SafeRelease(m_pdrgpulPartColumns);
	CRefCount::SafeRelease(m_pdrgpdrgpulKeys);
	m_pdrgpmdidIndices->Release();
	m_pdrgpmdidTriggers->Release();
	m_pdrgpmdidCheckConstraint->Release();
	CRefCount::SafeRelease(m_pmdpartcnstr);
	CRefCount::SafeRelease(m_phmululNonDroppedCols);
	CRefCount::SafeRelease(m_phmiulAttno2Pos);
	CRefCount::SafeRelease(m_pdrgpulNonDroppedCols);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this relation
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::Mdname
//
//	@doc:
//		Returns the name of this relation
//
//---------------------------------------------------------------------------
CMDName
CMDRelationGPDB::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::FTemporary
//
//	@doc:
//		Is the relation temporary
//
//---------------------------------------------------------------------------
BOOL
CMDRelationGPDB::FTemporary() const
{
	return m_fTemporary;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::Erelstorage
//
//	@doc:
//		Returns the storage type for this relation
//
//---------------------------------------------------------------------------
IMDRelation::Erelstoragetype
CMDRelationGPDB::Erelstorage() const
{
	return m_erelstorage;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::Ereldistribution
//
//	@doc:
//		Returns the distribution policy for this relation
//
//---------------------------------------------------------------------------
IMDRelation::Ereldistrpolicy
CMDRelationGPDB::Ereldistribution() const
{
	return m_ereldistrpolicy;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlColumns
//
//	@doc:
//		Returns the number of columns of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlColumns() const
{
	GPOS_ASSERT(NULL != m_pdrgpmdcol);
	
	return m_pdrgpmdcol->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::FHasDroppedColumns
//
//	@doc:
//		Does relation have dropped columns
//
//---------------------------------------------------------------------------
BOOL
CMDRelationGPDB::FHasDroppedColumns() const
{	
	return 0 < m_ulDroppedCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlNonDroppedCols
//
//	@doc:
//		Number of non-dropped columns
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlNonDroppedCols() const
{	
	return UlColumns() - m_ulDroppedCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlPosNonDropped
//
//	@doc:
//		Return the absolute position of the given attribute position excluding 
//		dropped columns
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlPosNonDropped
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
//		CMDRelationGPDB::UlPosFromAttno
//
//	@doc:
//		Return the position of a column in the metadata object given the
//      attribute number in the system catalog
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlPosFromAttno
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
//		CMDRelationGPDB::PdrgpulNonDroppedCols
//
//	@doc:
//		Returns the original positions of all the non-dropped columns
//
//---------------------------------------------------------------------------
DrgPul *
CMDRelationGPDB::PdrgpulNonDroppedCols() const
{
	return m_pdrgpulNonDroppedCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlKeySets
//
//	@doc:
//		Returns the number of key sets
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlKeySets() const
{	
	return m_pdrgpdrgpulKeys->UlSafeLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::PdrgpulKeyset
//
//	@doc:
//		Returns the key set at the specified position
//
//---------------------------------------------------------------------------
const DrgPul *
CMDRelationGPDB::PdrgpulKeyset
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
//		CMDRelationGPDB::UlDistrColumns
//
//	@doc:
//		Returns the number of columns in the distribution column list of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlDistrColumns() const
{	
	return m_pdrgpulDistrColumns->UlSafeLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::FHasOids
//
//	@doc:
//		Does this table have oids
//
//---------------------------------------------------------------------------
BOOL
CMDRelationGPDB::FHasOids() const
{
	return m_fHasOids;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::FPartitioned
//
//	@doc:
//		Is the table partitioned
//
//---------------------------------------------------------------------------
BOOL
CMDRelationGPDB::FPartitioned() const
{	
	return (0 < UlPartColumns());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlPartitions
//
//	@doc:
//		number of partitions
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlPartitions() const
{
	return m_ulPartitions;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlPartColumns
//
//	@doc:
//		Returns the number of partition keys
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlPartColumns() const
{	
	return m_pdrgpulPartColumns->UlSafeLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::PmdcolPartColumn
//
//	@doc:
//		Returns the partition column at the specified position in the
//		partition key list
//
//---------------------------------------------------------------------------
const IMDColumn *
CMDRelationGPDB::PmdcolPartColumn
	(
	ULONG ulPos
	) 
	const
{
	ULONG ulPartKeyPos = (*(*m_pdrgpulPartColumns)[ulPos]);
	return Pmdcol(ulPartKeyPos);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlIndices
//
//	@doc:
//		Returns the number of indices of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlIndices() const
{
	return m_pdrgpmdidIndices->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlTriggers
//
//	@doc:
//		Returns the number of triggers of this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlTriggers() const
{
	return m_pdrgpmdidTriggers->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::Pmdcol
//
//	@doc:
//		Returns the column at the specified position
//
//---------------------------------------------------------------------------
const IMDColumn *
CMDRelationGPDB::Pmdcol
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
//		CMDRelationGPDB::PmdcolDistrColumn
//
//	@doc:
//		Returns the distribution column at the specified position in the distribution column list
//
//---------------------------------------------------------------------------
const IMDColumn *
CMDRelationGPDB::PmdcolDistrColumn
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
//		CMDRelationGPDB::FConvertHashToRandom
//
//	@doc:
//		Return true if a hash distributed table needs to be considered as random during planning
//---------------------------------------------------------------------------
BOOL
CMDRelationGPDB::FConvertHashToRandom() const
{
	return m_fConvertHashToRandom;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::PmdidIndex
//
//	@doc:
//		Returns the id of the index at the specified position of the index array
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationGPDB::PmdidIndex
	(
	ULONG ulPos
	) 
	const
{
	return (*m_pdrgpmdidIndices)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::PmdidTrigger
//
//	@doc:
//		Returns the id of the trigger at the specified position of the trigger array
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationGPDB::PmdidTrigger
	(
	ULONG ulPos
	)
	const
{
	return (*m_pdrgpmdidTriggers)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::UlCheckConstraints
//
//	@doc:
//		Returns the number of check constraints on this relation
//
//---------------------------------------------------------------------------
ULONG
CMDRelationGPDB::UlCheckConstraints() const
{
	return m_pdrgpmdidCheckConstraint->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::PmdidCheckConstraint
//
//	@doc:
//		Returns the id of the check constraint at the specified position of
//		the check constraint array
//
//---------------------------------------------------------------------------
IMDId *
CMDRelationGPDB::PmdidCheckConstraint
	(
	ULONG ulPos
	)
	const
{
	return (*m_pdrgpmdidCheckConstraint)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::Pmdpartcnstr
//
//	@doc:
//		Return the part constraint
//
//---------------------------------------------------------------------------
IMDPartConstraint *
CMDRelationGPDB::Pmdpartcnstr() const
{
	return m_pmdpartcnstr;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDRelationGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	GPOS_CHECK_ABORT;

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenRelation));
	
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelTemporary), m_fTemporary);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelHasOids), m_fHasOids);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelStorageType), IMDRelation::PstrStorageType(m_erelstorage));
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
	
	if (FPartitioned())
	{
		// serialize partition keys
		CWStringDynamic *pstrPartKeys = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulPartColumns);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartKeys), pstrPartKeys);
		GPOS_DELETE(pstrPartKeys);
	}
	
	if (m_fConvertHashToRandom)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenConvertHashToRandom), m_fConvertHashToRandom);
	}

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenNumLeafPartitions), m_ulPartitions);

	// serialize columns
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumns));
	for (ULONG ul = 0; ul < m_pdrgpmdcol->UlLength(); ul++)
	{
		IMDColumn *pmdcol = (*m_pdrgpmdcol)[ul];
		pmdcol->Serialize(pxmlser);

		GPOS_CHECK_ABORT;
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

	// serialize part constraint
	if (NULL != m_pmdpartcnstr)
	{
		m_pmdpartcnstr->Serialize(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenRelation));

	GPOS_CHECK_ABORT;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDRelationGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDRelationGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Relation id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;
	
	os << "Relation name: " << (Mdname()).Pstr()->Wsz() << std::endl;
	
	os << "Storage type: " << IMDRelation::PstrStorageType(m_erelstorage)->Wsz() << std::endl;
	
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

	os << "Partition keys: ";
	const ULONG ulPartColumns = UlPartColumns(); 
	for (ULONG ul = 0; ul < ulPartColumns; ul++)
	{
		if (0 < ul)
		{
			os << ", ";
		}
		
		const IMDColumn *pmdcolPartKey = PmdcolPartColumn(ul);
		os << (pmdcolPartKey->Mdname()).Pstr()->Wsz();		
	}
		
	os << std::endl;
		
	os << "Indexes: ";
	CDXLUtils::DebugPrintDrgpmdid(os, m_pdrgpmdidIndices);

	os << "Triggers: ";
	CDXLUtils::DebugPrintDrgpmdid(os, m_pdrgpmdidTriggers);

	os << "Check Constraint: ";
	CDXLUtils::DebugPrintDrgpmdid(os, m_pdrgpmdidCheckConstraint);
}

#endif // GPOS_DEBUG

// EOF

