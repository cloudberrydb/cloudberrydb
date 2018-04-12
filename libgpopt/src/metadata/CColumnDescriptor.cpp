//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColumnDescriptor.cpp
//
//	@doc:
//		Column abstraction for tables, functions, external tables etc.
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/metadata/CColumnDescriptor.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CColumnDescriptor::CColumnDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CColumnDescriptor::CColumnDescriptor
	(
	IMemoryPool *pmp,
	const IMDType *pmdtype,
	INT iTypeModifier,
	OID oidCollation,
	const CName &name,
	INT iAttno,
	BOOL fNullable,
	ULONG ulWidth
	)
	:
	m_pmdtype(pmdtype),
	m_iTypeModifier(iTypeModifier),
	m_oidCollation(oidCollation),
	m_name(pmp, name),
	m_iAttno(iAttno),
	m_fNullable(fNullable),
	m_ulWidth(ulWidth)
{
	GPOS_ASSERT(NULL != pmdtype);
	GPOS_ASSERT(pmdtype->Pmdid()->FValid());

	if (m_pmdtype->FFixedLength())
	{
		ulWidth = m_pmdtype->UlLength();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnDescriptor::~CColumnDescriptor
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CColumnDescriptor::~CColumnDescriptor()
{}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CColumnDescriptor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CColumnDescriptor::OsPrint
	(
	IOstream &os
	)
	const
{
	return m_name.OsPrint(os);
}

#endif // GPOS_DEBUG

// EOF

