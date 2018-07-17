//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CIndexDescriptor.cpp
//
//	@doc:
//		Implementation of index description
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CIndexDescriptor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::CIndexDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CIndexDescriptor::CIndexDescriptor
	(
	IMemoryPool *pmp,
	IMDId *pmdidIndex,
	const CName &name,
	DrgPcoldesc *pdrgcoldescKeyCols,
	DrgPcoldesc *pdrgcoldescIncludedCols,
	BOOL fClustered,
	IMDIndex::EmdindexType emdindtype
	)
	:
	m_pmdidIndex(pmdidIndex),
	m_name(pmp, name),
	m_pdrgpcoldescKeyCols(pdrgcoldescKeyCols),
	m_pdrgpcoldescIncludedCols(pdrgcoldescIncludedCols),
	m_fClustered(fClustered),
	m_emdindt(emdindtype)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(pmdidIndex->FValid());
	GPOS_ASSERT(NULL != pdrgcoldescKeyCols);
	GPOS_ASSERT(NULL != pdrgcoldescIncludedCols);
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::~CIndexDescriptor
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CIndexDescriptor::~CIndexDescriptor()
{
	m_pmdidIndex->Release();

	m_pdrgpcoldescKeyCols->Release();
	m_pdrgpcoldescIncludedCols->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::UlKeys
//
//	@doc:
//		number of key columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::UlKeys() const
{
	return m_pdrgpcoldescKeyCols->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::UlIncludedColumns
//
//	@doc:
//		Number of included columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::UlIncludedColumns() const
{
	// array allocated in ctor
	GPOS_ASSERT(NULL != m_pdrgpcoldescIncludedCols);

	return m_pdrgpcoldescIncludedCols->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Pindexdesc
//
//	@doc:
//		Create the index descriptor from the table descriptor and index
//		information from the catalog
//
//---------------------------------------------------------------------------
CIndexDescriptor *
CIndexDescriptor::Pindexdesc
	(
	IMemoryPool *pmp,
	const CTableDescriptor *ptabdesc,
	const IMDIndex *pmdindex
	)
{
	CWStringConst strIndexName(pmp, pmdindex->Mdname().Pstr()->Wsz());

	DrgPcoldesc *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();

	pmdindex->Pmdid()->AddRef();

	// array of index column descriptors
	DrgPcoldesc *pdrgcoldescKey = GPOS_NEW(pmp) DrgPcoldesc(pmp);

	for (ULONG ul = 0; ul < pmdindex->UlKeys(); ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescKey->Append(pcoldesc);
	}

	// array of included column descriptors
	DrgPcoldesc *pdrgcoldescIncluded = GPOS_NEW(pmp) DrgPcoldesc(pmp);
	for (ULONG ul = 0; ul < pmdindex->UlIncludedCols(); ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescIncluded->Append(pcoldesc);
	}


	// create the index descriptors
	CIndexDescriptor *pindexdesc = GPOS_NEW(pmp) CIndexDescriptor
											(
											pmp,
											pmdindex->Pmdid(),
											CName(&strIndexName),
											pdrgcoldescKey,
											pdrgcoldescIncluded,
											pmdindex->FClustered(),
											pmdindex->Emdindt()
											);
	return pindexdesc;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CIndexDescriptor::OsPrint
	(
	IOstream &os
	)
	const
{
	m_name.OsPrint(os);
	os << ": (Keys :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescKeyCols, m_pdrgpcoldescKeyCols->UlLength());
	os << "); ";

	os << "(Included Columns :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescIncludedCols, m_pdrgpcoldescIncludedCols->UlLength());
	os << ")";

	os << " [ Clustered :";
	if (m_fClustered)
	{
		os << "true";
	}
	else
	{
		os << "false";
	}
	os << " ]";
	return os;
}

#endif // GPOS_DEBUG

// EOF

