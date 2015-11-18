//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CRandom.cpp
//
//	@doc:
//		Random number generator.
//
//	@owner:
//		Siva
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/common/CRandom.h"

#define DEFAULT_SEED 102

using namespace gpos;

CRandom::CRandom()
	:m_iSeed(DEFAULT_SEED)
{
}


CRandom::CRandom(ULONG seed)
	:m_iSeed(seed)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CRandom::Next
//
//	@doc:
//		Returns next random number in the range 0 - 2^32
//
//---------------------------------------------------------------------------

ULONG CRandom::ULNext()
{
	return clib::UlRandR(&m_iSeed);
}

CRandom::~CRandom()
{
}

// EOF

