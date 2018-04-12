//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CColRefTable.cpp
//
//	@doc:
//		Implementation of column reference class
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CColRefTable.h"

#include "naucrates/md/CMDIdGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::CColRefTable
//
//	@doc:
//		Ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRefTable::CColRefTable
	(
	const CColumnDescriptor *pcoldesc,
	ULONG ulId,
	const CName *pname,
	ULONG ulOpSource
	)
	:
	CColRef(pcoldesc->Pmdtype(), pcoldesc->ITypeModifier(), pcoldesc->OidCollation(), ulId, pname),
	m_iAttno(0),
	m_ulSourceOpId(ulOpSource),
	m_ulWidth(pcoldesc->UlWidth())
{
	GPOS_ASSERT(NULL != pname);

	m_iAttno = pcoldesc->IAttno();
	m_fNullable = pcoldesc->FNullable();
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::CColRefTable
//
//	@doc:
//		Ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRefTable::CColRefTable
	(
	const IMDType *pmdtype,
	INT iTypeModifier,
	OID oidCollation,
	INT iAttno,
	BOOL fNullable,
	ULONG ulId,
	const CName *pname,
	ULONG ulOpSource,
	ULONG ulWidth
	)
	:
	CColRef(pmdtype, iTypeModifier, oidCollation, ulId, pname),
	m_iAttno(iAttno),
	m_fNullable(fNullable),
	m_ulSourceOpId(ulOpSource),
	m_ulWidth(ulWidth)
{
	GPOS_ASSERT(NULL != pname);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::~CColRefTable
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CColRefTable::~CColRefTable()
{}


// EOF

