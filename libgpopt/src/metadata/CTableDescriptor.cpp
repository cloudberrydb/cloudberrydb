//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CTableDescriptor.cpp
//
//	@doc:
//		Implementation of table abstraction
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CTableDescriptor.h"

#include "naucrates/exception.h"
#include "naucrates/md/IMDIndex.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::CTableDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CTableDescriptor::CTableDescriptor
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	const CName &name,
	BOOL fConvertHashToRandom,
	IMDRelation::Ereldistrpolicy ereldistrpolicy,
	IMDRelation::Erelstoragetype erelstoragetype,
	ULONG ulExecuteAsUser
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_name(pmp, name),
	m_pdrgpcoldesc(NULL),
	m_ereldistrpolicy(ereldistrpolicy),
	m_erelstoragetype(erelstoragetype),
	m_pdrgpcoldescDist(NULL),
	m_fConvertHashToRandom(fConvertHashToRandom),
	m_pdrgpulPart(NULL),
	m_pdrgpbsKeys(NULL),
	m_ulPartitions(0),
	m_ulExecuteAsUser(ulExecuteAsUser),
	m_fHasPartialIndexes(FDescriptorWithPartialIndexes())
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(pmdid->FValid());
	
	m_pdrgpcoldesc = GPOS_NEW(m_pmp) DrgPcoldesc(m_pmp);
	m_pdrgpcoldescDist = GPOS_NEW(m_pmp) DrgPcoldesc(m_pmp);
	m_pdrgpulPart = GPOS_NEW(m_pmp) DrgPul(m_pmp);
	m_pdrgpbsKeys = GPOS_NEW(m_pmp) DrgPbs(m_pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::~CTableDescriptor
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CTableDescriptor::~CTableDescriptor()
{
	m_pmdid->Release();
	
	m_pdrgpcoldesc->Release();
	m_pdrgpcoldescDist->Release();
	m_pdrgpulPart->Release();
	m_pdrgpbsKeys->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::UlColumns
//
//	@doc:
//		number of columns
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::UlColumns() const
{
	// array allocated in ctor
	GPOS_ASSERT(NULL != m_pdrgpcoldesc);
	
	return m_pdrgpcoldesc->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::UlPos
//
//	@doc:
//		Find the position of a column descriptor in an array of column descriptors.
//		If not found, return the size of the array
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::UlPos
	(
	const CColumnDescriptor *pcoldesc,
	const DrgPcoldesc *pdrgpcoldesc
	)
	const
{
	GPOS_ASSERT(NULL != pcoldesc);
	GPOS_ASSERT(NULL != pdrgpcoldesc);
	
	ULONG ulArity = pdrgpcoldesc->UlLength();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (pcoldesc == (*pdrgpcoldesc)[ul])
		{
			return ul;
		}
	}
	
	return ulArity;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::UlPosition
//
//	@doc:
//		Find the position of the attribute in the array of column descriptors
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::UlPosition
	(
	INT iAttno
	)
	const
{
	GPOS_ASSERT(NULL != m_pdrgpcoldesc);
	ULONG ulPos = gpos::ulong_max;
	ULONG ulArity = m_pdrgpcoldesc->UlLength();

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CColumnDescriptor *pcoldesc = (*m_pdrgpcoldesc)[ul];
		if (pcoldesc->IAttno() == iAttno)
		{
			ulPos = ul;
		}
	}
	GPOS_ASSERT(gpos::ulong_max != ulPos);

	return ulPos;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::AddColumn
//
//	@doc:
//		Add column to table descriptor
//
//---------------------------------------------------------------------------
void
CTableDescriptor::AddColumn
	(
	CColumnDescriptor *pcoldesc
	)
{
	GPOS_ASSERT(NULL != pcoldesc);
	
	m_pdrgpcoldesc->Append(pcoldesc);
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::AddDistributionColumn
//
//	@doc:
//		Add the column at the specified position to the array of column 
//		descriptors defining a hash distribution
//
//---------------------------------------------------------------------------
void
CTableDescriptor::AddDistributionColumn
	(
	ULONG ulPos
	)
{
	CColumnDescriptor *pcoldesc = (*m_pdrgpcoldesc)[ulPos];
	pcoldesc->AddRef();
	m_pdrgpcoldescDist->Append(pcoldesc);
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::AddPartitionColumn
//
//	@doc:
//		Add the column at the specified position to the array of partition column 
//		descriptors
//
//---------------------------------------------------------------------------
void
CTableDescriptor::AddPartitionColumn
	(
	ULONG ulPos
	)
{
	m_pdrgpulPart->Append(GPOS_NEW(m_pmp) ULONG(ulPos));
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::FAddKeySet
//
//	@doc:
//		Add a keyset, returns true if key set is successfully added
//
//---------------------------------------------------------------------------
BOOL
CTableDescriptor::FAddKeySet
	(
	CBitSet *pbs
	)
{
	GPOS_ASSERT(NULL != pbs);
	GPOS_ASSERT(pbs->CElements() <= m_pdrgpcoldesc->UlLength());
	
	const ULONG ulSize = m_pdrgpbsKeys->UlLength();
	BOOL fFound = false;
	for (ULONG ul = 0; !fFound && ul < ulSize; ul++)
	{
		CBitSet *pbsCurrent = (*m_pdrgpbsKeys)[ul];
		fFound = pbsCurrent->FEqual(pbs);
	}

	if (!fFound)
	{
		m_pdrgpbsKeys->Append(pbs);
	}

	return !fFound;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::Pcoldesc
//
//	@doc:
//		Get n-th column descriptor
//
//---------------------------------------------------------------------------
const CColumnDescriptor *
CTableDescriptor::Pcoldesc
	(
	ULONG ulCol
	)
	const
{
	GPOS_ASSERT(ulCol < UlColumns());
	
	return (*m_pdrgpcoldesc)[ulCol];
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CTableDescriptor::OsPrint
	(
	IOstream &os
	)
	const
{
	m_name.OsPrint(os);
	os << ": (";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldesc, m_pdrgpcoldesc->UlLength());
	os << ")";
	return os;
}

#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::UlIndices
//
//	@doc:
//		 Returns number of b-tree indices
//
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::UlIndices()
{
	GPOS_ASSERT(NULL != m_pmdid);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(m_pmdid);
	const ULONG ulIndices = pmdrel->UlIndices();

	return ulIndices;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::UlPartitions
//
//	@doc:
//		 Returns number of leaf partitions
//
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::UlPartitions()
	const
{
	GPOS_ASSERT(NULL != m_pmdid);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(m_pmdid);
	const ULONG ulPartitions = pmdrel->UlPartitions();

	return ulPartitions;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::FDescriptorWithPartialIndexes
//
//	@doc:
//		Returns true if this given table descriptor has partial indexes
//
//---------------------------------------------------------------------------
BOOL
CTableDescriptor::FDescriptorWithPartialIndexes()
{
	const ULONG ulIndices = UlIndices();
	if (0 == ulIndices)
	{
		return false;
	}

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(m_pmdid);
	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		if (pmdrel->FPartialIndex(pmdrel->PmdidIndex(ul)))
		{
			return true;
		}
	}

	return false;
}

// EOF

