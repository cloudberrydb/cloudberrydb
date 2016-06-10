//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDXLMinidump.cpp
//
//	@doc:
//		Implementation of DXL-based minidump object
//---------------------------------------------------------------------------

#include "gpos/common/CBitSet.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "gpopt/minidump/CDXLMinidump.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::CDXLMinidump
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLMinidump::CDXLMinidump
	(
	CBitSet *pbs,
	COptimizerConfig *poconf,
	CDXLNode *pdxlnQuery, 
	DrgPdxln *pdrgpdxlnQueryOutput,
	DrgPdxln *pdrgpdxlnCTE,
	CDXLNode *pdxlnPlan, 
	DrgPimdobj *pdrgpmdobj,
	DrgPsysid *pdrgpsysid,
	ULLONG ullPlanId,
	ULLONG ullPlanSpaceSize
	)
	:
	m_pbs(pbs),
	m_poconf(poconf),
	m_pdxlnQuery(pdxlnQuery),
	m_pdrgpdxlnQueryOutput(pdrgpdxlnQueryOutput),
	m_pdrgpdxlnCTE(pdrgpdxlnCTE),
	m_pdxlnPlan(pdxlnPlan),
	m_pdrgpmdobj(pdrgpmdobj),
	m_pdrgpsysid(pdrgpsysid),
	m_ullPlanId(ullPlanId),
	m_ullPlanSpaceSize(ullPlanSpaceSize)
{}


//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::~CDXLMinidump
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLMinidump::~CDXLMinidump()
{
	// some of the structures may be NULL as they are not included in the minidump
	CRefCount::SafeRelease(m_pbs);
	CRefCount::SafeRelease(m_poconf);
	CRefCount::SafeRelease(m_pdxlnQuery);
	CRefCount::SafeRelease(m_pdrgpdxlnQueryOutput);
	CRefCount::SafeRelease(m_pdrgpdxlnCTE);
	CRefCount::SafeRelease(m_pdxlnPlan);
	CRefCount::SafeRelease(m_pdrgpmdobj);
	CRefCount::SafeRelease(m_pdrgpsysid);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::Pbs
//
//	@doc:
//		Traceflags
//
//---------------------------------------------------------------------------
const CBitSet *
CDXLMinidump::Pbs() const
{
	return m_pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::PdxlnQuery
//
//	@doc:
//		Query object
//
//---------------------------------------------------------------------------
const CDXLNode *
CDXLMinidump::PdxlnQuery() const
{
	return m_pdxlnQuery;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::PdrgpdxlnQueryOutput
//
//	@doc:
//		Query output columns
//
//---------------------------------------------------------------------------
const DrgPdxln *
CDXLMinidump::PdrgpdxlnQueryOutput() const
{
	return m_pdrgpdxlnQueryOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::PdrgpdxlnCTE
//
//	@doc:
//		CTE list
//
//---------------------------------------------------------------------------
const DrgPdxln *
CDXLMinidump::PdrgpdxlnCTE() const
{
	return m_pdrgpdxlnCTE;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::PdxlnPlan
//
//	@doc:
//		Query object
//
//---------------------------------------------------------------------------
const CDXLNode *
CDXLMinidump::PdxlnPlan() const
{
	return m_pdxlnPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::Pdrgpmdobj
//
//	@doc:
//		Metadata objects
//
//---------------------------------------------------------------------------
const DrgPimdobj *
CDXLMinidump::Pdrgpmdobj() const
{
	return m_pdrgpmdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::Pdrgpsysid
//
//	@doc:
//		Metadata source system ids
//
//---------------------------------------------------------------------------
const DrgPsysid *
CDXLMinidump::Pdrgpsysid() const
{
	return m_pdrgpsysid;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::UllPlanId
//
//	@doc:
//		Returns plan id
//
//---------------------------------------------------------------------------
ULLONG
CDXLMinidump::UllPlanId() const
{
	return m_ullPlanId;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLMinidump::UllPlanId
//
//	@doc:
//		Returns plan space size
//
//---------------------------------------------------------------------------
ULLONG
CDXLMinidump::UllPlanSpaceSize() const
{
	return m_ullPlanSpaceSize;
}


// EOF
