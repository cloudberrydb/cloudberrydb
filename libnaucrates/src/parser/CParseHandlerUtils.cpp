//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerUtils.cpp
//
//	@doc:
//		Implementation of the helper methods for parse handler
//		
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/statistics/IStatistics.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerUtils::SetProperties
//
//	@doc:
//		Parse and the set operator's costing and statistical properties
//
//---------------------------------------------------------------------------
void
CParseHandlerUtils::SetProperties
	(
	CDXLNode *pdxln,
	CParseHandlerProperties *pphProp
	)
{
	GPOS_ASSERT(NULL != pphProp->Pdxlprop());
	// set physical properties
	CDXLPhysicalProperties *pdxlprop = pphProp->Pdxlprop();
	pdxlprop->AddRef();
	pdxln->SetProperties(pdxlprop);

	// set the statistical information
	CDXLStatsDerivedRelation *pdxlstatsderrel = pphProp->Pdxlstatsderrel();
	if (NULL != pdxlstatsderrel)
	{
		pdxlstatsderrel->AddRef();
		pdxlprop->SetStats(pdxlstatsderrel);
	}
}

// EOF
