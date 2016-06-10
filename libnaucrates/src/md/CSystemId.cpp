//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSystemId.cpp
//
//	@doc:
//		Implementation of system identifiers
//---------------------------------------------------------------------------


#include "naucrates/md/CSystemId.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::CSystemId
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CSystemId::CSystemId
	(
	IMDId::EMDIdType emdidt,
	const WCHAR *wsz,
	ULONG ulLength
	)
	:
	m_emdidt(emdidt)
{
	GPOS_ASSERT(GPDXL_SYSID_LENGTH >= ulLength);

	if (ulLength > 0)
	{
		clib::WszWcsNCpy(m_wsz, wsz, ulLength);
	}
	
	// ensure string is terminated
	m_wsz[ulLength] = WCHAR_EOS;
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::CSystemId
//
//	@doc:
//		Copy constructor
//
//---------------------------------------------------------------------------
CSystemId::CSystemId
	(
	const CSystemId &sysid
	)
	:
	m_emdidt(sysid.Emdidt())
{
	clib::WszWcsNCpy(m_wsz, sysid.Wsz(), GPDXL_SYSID_LENGTH);
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::FEquals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CSystemId::FEquals
	(
	const CSystemId &sysid
	)
	const
{
	ULONG ulLength = GPOS_WSZ_LENGTH(m_wsz);
	return ulLength == GPOS_WSZ_LENGTH(sysid.m_wsz) &&
			0 == clib::IWcsNCmp(m_wsz, sysid.m_wsz, ulLength);
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CSystemId::UlHash() const
{
	return gpos::UlHashByteArray((BYTE*) m_wsz, GPOS_WSZ_LENGTH(m_wsz) * GPOS_SIZEOF(WCHAR));
}

// EOF
