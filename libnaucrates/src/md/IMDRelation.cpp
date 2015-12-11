//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		IMDRelation.cpp
//
//	@doc:
//		Implementation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDRelation.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

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
IMDRelation::PstrDistrPolicy
	(
	Ereldistrpolicy ereldistrpolicy
	)
{
	switch (ereldistrpolicy)
	{
		case EreldistrMasterOnly:
			return CDXLTokens::PstrToken(EdxltokenRelDistrMasterOnly);
		case EreldistrHash:
			return CDXLTokens::PstrToken(EdxltokenRelDistrHash);
		case EreldistrRandom:
			return CDXLTokens::PstrToken(EdxltokenRelDistrRandom);
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		IMDRelation::PstrStorageType
//
//	@doc:
//		Return name of storage type
//
//---------------------------------------------------------------------------
const CWStringConst *
IMDRelation::PstrStorageType
	(
	IMDRelation::Erelstoragetype erelstorage
	)
{
	switch (erelstorage)
	{
		case ErelstorageHeap:
			return CDXLTokens::PstrToken(EdxltokenRelStorageHeap);
		case ErelstorageAppendOnlyCols:
			return CDXLTokens::PstrToken(EdxltokenRelStorageAppendOnlyCols);
		case ErelstorageAppendOnlyRows:
			return CDXLTokens::PstrToken(EdxltokenRelStorageAppendOnlyRows);
		case ErelstorageAppendOnlyParquet:
			return CDXLTokens::PstrToken(EdxltokenRelStorageAppendOnlyParquet);
		case ErelstorageExternal:
			return CDXLTokens::PstrToken(EdxltokenRelStorageExternal);
		case ErelstorageVirtual:
			return CDXLTokens::PstrToken(EdxltokenRelStorageVirtual);
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		IMDRelation::PstrColumns
//
//	@doc:
//		Serialize an array of column ids into a comma-separated string
//
//---------------------------------------------------------------------------
CWStringDynamic *
IMDRelation::PstrColumns
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpul
	)
{
	CWStringDynamic *pstr = GPOS_NEW(pmp) CWStringDynamic(pmp);

	ULONG ulLen = pdrgpul->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULONG ulId = *((*pdrgpul)[ul]);
		if (ul == ulLen - 1)
		{
			// last element: do not print a comma
			pstr->AppendFormat(GPOS_WSZ_LIT("%d"), ulId);
		}
		else
		{
			pstr->AppendFormat(GPOS_WSZ_LIT("%d%ls"), ulId, CDXLTokens::PstrToken(EdxltokenComma)->Wsz());
		}
	}

	return pstr;
}

// EOF
