//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		IMDIndex.cpp
//
//	@doc:
//		Implementation of MD index
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDIndex.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDRelation::PstrDistrPolicy
//
//	@doc:
//		Return relation distribution policy as a string value
//
//---------------------------------------------------------------------------
const CWStringConst *
IMDIndex::PstrIndexType
	(
	EmdindexType emdindt
	)
{
	switch (emdindt)
	{
		case EmdindBtree:
			return CDXLTokens::PstrToken(EdxltokenIndexTypeBtree);
		case EmdindBitmap:
			return CDXLTokens::PstrToken(EdxltokenIndexTypeBitmap);
		default:
			GPOS_ASSERT(!"Unrecognized index type");
			return NULL;
	}
}

// EOF
