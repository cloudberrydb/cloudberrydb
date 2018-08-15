//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Greenplum, Inc.
//
//	@filename:
//		CCatalogUtils.cpp
//
//	@doc:
//		Routines to extract interesting information from the catalog
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/utils/CCatalogUtils.h"

//---------------------------------------------------------------------------
//	@function:
//		CCatalogUtils::PlRelationOids
//
//	@doc:
//		Return list of relation oids from the catalog
//
//---------------------------------------------------------------------------
List *
CCatalogUtils::PlRelationOids()
{
	return relation_oids();
}

//---------------------------------------------------------------------------
//	@function:
//		CCatalogUtils::PlOperatorOids
//
//	@doc:
//		Return list of operator oids from the catalog
//
//---------------------------------------------------------------------------
List *
CCatalogUtils::PlOperatorOids()
{
	return operator_oids();
}

//---------------------------------------------------------------------------
//	@function:
//		CCatalogUtils::PlFunctionOids
//
//	@doc:
//		Return list of function plOids from the catalog
//
//---------------------------------------------------------------------------
List *
CCatalogUtils::PlFunctionOids()
{
	return function_oids();
}

//---------------------------------------------------------------------------
//	@function:
//		CCatalogUtils::PlAllOids
//
//	@doc:
//		Return list of all plOids from the catalog
//
//---------------------------------------------------------------------------
List *CCatalogUtils::PlAllOids()
{
	return list_concat(list_concat(PlRelationOids(), PlOperatorOids()), PlFunctionOids());
}

// EOF
