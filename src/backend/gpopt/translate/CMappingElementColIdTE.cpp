//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMappingElementColIdTE.cpp
//
//	@doc:
//		Implementation of the functions that provide the mapping from CDXLNode to
//		Var during DXL->Query translation
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"

#include "gpopt/translate/CMappingElementColIdTE.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMappingElementColIdTE::CMappingElementColIdTE
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CMappingElementColIdTE::CMappingElementColIdTE
	(
	ULONG ulColId,
	ULONG ulQueryLevel,
	TargetEntry *pte
	)
	:
	m_ulColId(ulColId),
	m_ulQueryLevel(ulQueryLevel),
	m_pte(pte)
{
}

// EOF
