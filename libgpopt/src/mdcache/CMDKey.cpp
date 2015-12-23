//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDKey.cpp
//
//	@doc:
//		Implementation of a key for metadata cache objects
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/io/COstreamString.h"

#include "gpopt/mdcache/CMDKey.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CMDKey::CMDKey
//
//	@doc:
//		Constructs a md cache key
//
//---------------------------------------------------------------------------
CMDKey::CMDKey
	(
	const IMDId *pmdid
	)
	:
	m_pmdid(pmdid)
{
	GPOS_ASSERT(pmdid->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::FEquals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CMDKey::FEquals
	(
	const CMDKey &mdkey
	)
	const
{	
	return mdkey.Pmdid()->FEquals(m_pmdid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::FEqualMDKey
//
//	@doc:
//		Equality function for using MD keys in a cache
//
//---------------------------------------------------------------------------
BOOL
CMDKey::FEqualMDKey
	(
	CMDKey* const &pvLeft,
	CMDKey* const &pvRight
	)
{
	if (NULL == pvLeft && NULL == pvRight)
	{
		return true;
	}

	if (NULL == pvLeft || NULL == pvRight)
	{
		return false;
	}
	
	GPOS_ASSERT(NULL != pvLeft && NULL != pvRight);
	
	return pvLeft->Pmdid()->FEquals(pvRight->Pmdid());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG 
CMDKey::UlHash() const
{
	return m_pmdid->UlHash();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::UlHashMDKey
//
//	@doc:
//		Hash function for using MD keys in a cache
//
//---------------------------------------------------------------------------
ULONG 
CMDKey::UlHashMDKey
	(
	CMDKey* const & pv
	)
{
	return pv->Pmdid()->UlHash();
}

// EOF
