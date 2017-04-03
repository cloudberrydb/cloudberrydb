//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSerializablePlan.cpp
//
//	@doc:
//		Serializable plan object
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/task/CTask.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/minidump/CSerializablePlan.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CSerializablePlan::CSerializablePlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSerializablePlan::CSerializablePlan
	(
	IMemoryPool *pmp,
	const CDXLNode *pdxlnPlan,
	ULLONG ullPlanId,
	ULLONG ullPlanSpaceSize
	)
	:
	CSerializable(),
	m_pmp(pmp),
	m_pdxlnPlan(pdxlnPlan),
	m_pstrPlan(NULL),
	m_ullPlanId(ullPlanId),
	m_ullPlanSpaceSize(ullPlanSpaceSize)
{
	GPOS_ASSERT(NULL != pdxlnPlan);
}


//---------------------------------------------------------------------------
//	@function:
//		CSerializablePlan::~CSerializablePlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSerializablePlan::~CSerializablePlan()
{
	GPOS_DELETE(m_pstrPlan);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializablePlan::Serialize
//
//	@doc:
//		Serialize contents into provided stream
//
//---------------------------------------------------------------------------
void
CSerializablePlan::Serialize
	(
	COstream &oos
	)
{
	CDXLUtils::SerializePlan
				(
				m_pmp,
				oos,
				m_pdxlnPlan,
				m_ullPlanId,
				m_ullPlanSpaceSize,
				false /*fSerializeHeaders*/,
				false /*fIndent*/
				);
}

// EOF

