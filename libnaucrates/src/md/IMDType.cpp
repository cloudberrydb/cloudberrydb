//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		IMDType.cpp
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

#include "gpos/string/CWStringConst.h"

#include "naucrates/base/IDatumStatisticsMappable.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDType::PstrCmpType
//
//	@doc:
//		Return the comparison type as a string value
//
//---------------------------------------------------------------------------
const CWStringConst *
IMDType::PstrCmpType
	(
	IMDType::ECmpType ecmpt
	)
{
	GPOS_ASSERT(IMDType::EcmptOther >= ecmpt);
	
	Edxltoken rgdxltoken[] = {EdxltokenCmpEq, EdxltokenCmpNeq, EdxltokenCmpLt, EdxltokenCmpLeq, EdxltokenCmpGt, EdxltokenCmpGeq, EdxltokenCmpIDF, EdxltokenCmpOther};
	
	GPOS_ASSERT(IMDType::EcmptOther + 1 == GPOS_ARRAY_SIZE(rgdxltoken));
	return CDXLTokens::PstrToken(rgdxltoken[ecmpt]);
}


//---------------------------------------------------------------------------
//	@function:
//		IMDType::FStatsComparable
//
//	@doc:
//		Return true if we can perform statistical comparison between
//		datums of these two types; else return false
//
//---------------------------------------------------------------------------
BOOL
IMDType::FStatsComparable
	(
	const IMDType *pmdtypeFst,
	const IMDType *pmdtypeSnd
	)
{
	GPOS_ASSERT(NULL != pmdtypeFst);
	GPOS_ASSERT(NULL != pmdtypeSnd);

	const IDatum *pdatumFst = pmdtypeFst->PdatumNull();
	const IDatum *pdatumSnd = pmdtypeSnd->PdatumNull();

	return pdatumFst->FStatsComparable(pdatumSnd);
}


//---------------------------------------------------------------------------
//	@function:
//		IMDType::FStatsComparable
//
//	@doc:
//		Return true if we can perform statistical comparison between
//		datum of the given type and a given datum; else return false
//
//---------------------------------------------------------------------------
BOOL
IMDType::FStatsComparable
	(
	const IMDType *pmdtypeFst,
	const IDatum *pdatumSnd
	)
{
	GPOS_ASSERT(NULL != pmdtypeFst);
	GPOS_ASSERT(NULL != pdatumSnd);

	const IDatum *pdatumFst = pmdtypeFst->PdatumNull();

	return pdatumFst->FStatsComparable(pdatumSnd);
}


// EOF
