//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSerializablePlan.cpp
//
//	@doc:
//		Serializable plan object
//
//	@owner:
//		
//
//	@test:
//
//
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
	const CDXLNode *pdxlnPlan,
	ULLONG ullPlanId,
	ULLONG ullPlanSpaceSize
	)
	:
	CSerializable(),
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
//		Serialize plan to string
//
//---------------------------------------------------------------------------
void
CSerializablePlan::Serialize
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL == m_pstrPlan);

	m_pstrPlan = CDXLUtils::PstrSerializePlan
				(
				pmp,
				m_pdxlnPlan,
				m_ullPlanId,
				m_ullPlanSpaceSize,
				false /*fSerializeHeaders*/,
				false /*fIndent*/
				);
}


//---------------------------------------------------------------------------
//	@function:
//		CSerializablePlan::UlpSerialize
//
//	@doc:
//		Serialize contents into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializablePlan::UlpSerialize
	(
	WCHAR *wszBuffer, 
	ULONG_PTR ulpAllocSize
	)
{
	ULONG_PTR ulpRequiredSpace = UlpRequiredSpace();
	
	GPOS_RTL_ASSERT(ulpAllocSize >= ulpRequiredSpace);
	
	if (0 < ulpRequiredSpace)
	{
		(void) clib::PvMemCpy
			(
			(BYTE *) wszBuffer,
			(BYTE *) m_pstrPlan->Wsz(),
			ulpRequiredSpace * GPOS_SIZEOF(WCHAR)
			);
	}
	
	return ulpRequiredSpace;
}

// EOF

