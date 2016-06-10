//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CIdGenerator.cpp
//
//	@doc:
//		Implementing the ULONG Counter
//---------------------------------------------------------------------------

#include "naucrates/dxl/CIdGenerator.h"
#include "gpos/base.h"

using namespace gpdxl;
using namespace gpos;

CIdGenerator::CIdGenerator(ULONG ulStartId)
	:m_ulId(ulStartId)
	{
	}

//---------------------------------------------------------------------------
//	@function:
//		CIdGenerator::UlNextId
//
//	@doc:
//		Returns the next unique id
//
//---------------------------------------------------------------------------
ULONG
CIdGenerator::UlNextId()
{
	return m_ulId++;
}

//---------------------------------------------------------------------------
//	@function:
//		CIdGenerator::UlCurrentId
//
//	@doc:
//		Returns the current unique id used
//
//---------------------------------------------------------------------------
ULONG
CIdGenerator::UlCurrentId()
{
	return m_ulId;
}


// EOF
