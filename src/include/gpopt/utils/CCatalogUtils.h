//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CCatalogUtils.h
//
//	@doc:
//		Routines to extract interesting information from the catalog
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef CCatalogUtils_H
#define CCatalogUtils_H

#include "gpdbdefs.h"


class CCatalogUtils {

	private:
		// return list of relation plOids in catalog
		static
		List *PlRelationOids();

		// return list of operator plOids in catalog
		static
		List *PlOperatorOids();

		// return list of function plOids in catalog
		static
		List *PlFunctionOids();

	public:

		// return list of all object plOids in catalog
		static
		List *PlAllOids();
};

#endif // CCatalogUtils_H

// EOF
