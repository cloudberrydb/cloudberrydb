//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		traceflags.cpp
//
//	@doc:
//		Implementation of trace flags routines
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "naucrates/traceflags/traceflags.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		SetTraceflags
//
//	@doc:
//		Set trace flags based on given bit set, and return two output bit
//		sets of old trace flags values
//
//---------------------------------------------------------------------------
void SetTraceflags
	(
	IMemoryPool *pmp,
	const CBitSet *pbsInput, // set of trace flags to be enabled
	CBitSet **ppbsEnabled,   // output: enabled trace flags before function is called
	CBitSet **ppbsDisabled   // output: disabled trace flags before function is called
	)
{
	if (NULL == pbsInput)
	{
		// bail out if input set is null
		return;
	}

	GPOS_ASSERT(NULL != ppbsEnabled);
	GPOS_ASSERT(NULL != ppbsDisabled);

	// suppress error simulation while setting trace flags
	CAutoTraceFlag atf1(EtraceSimulateAbort, false);
	CAutoTraceFlag atf2(EtraceSimulateOOM, false);
	CAutoTraceFlag atf3(EtraceSimulateNetError, false);
	CAutoTraceFlag atf4(EtraceSimulateIOError, false);

	*ppbsEnabled = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);
	*ppbsDisabled = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);
	CBitSetIter bsiter(*pbsInput);
	while (bsiter.FAdvance())
	{
		ULONG ulTraceFlag = bsiter.UlBit();
		if (GPOS_FTRACE(ulTraceFlag))
		{
			// set trace flag in the enabled set
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif	// GPOS_DEBUG
				(*ppbsEnabled)->FExchangeSet(ulTraceFlag);
			GPOS_ASSERT(!fSet);
		}
		else
		{
			// set trace flag in the disabled set
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif	// GPOS_DEBUG
				(*ppbsDisabled)->FExchangeSet(ulTraceFlag);
			GPOS_ASSERT(!fSet);
		}

		// set trace flag
		GPOS_SET_TRACE(ulTraceFlag);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ResetTraceflags
//
//	@doc:
//		Reset trace flags based on values given by input sets
//
//---------------------------------------------------------------------------
void ResetTraceflags
	(
	CBitSet *pbsEnabled,
	CBitSet *pbsDisabled
	)
{
	if (NULL == pbsEnabled || NULL == pbsDisabled)
	{
		// bail out if input sets are null
		return;
	}

	GPOS_ASSERT(NULL != pbsEnabled);
	GPOS_ASSERT(NULL != pbsDisabled);

	// suppress error simulation while resetting trace flags
	CAutoTraceFlag atf1(EtraceSimulateAbort, false);
	CAutoTraceFlag atf2(EtraceSimulateOOM, false);
	CAutoTraceFlag atf3(EtraceSimulateNetError, false);
	CAutoTraceFlag atf4(EtraceSimulateIOError, false);

	CBitSetIter bsiterEnabled(*pbsEnabled);
	while (bsiterEnabled.FAdvance())
	{
		ULONG ulTraceFlag = bsiterEnabled.UlBit();
		GPOS_SET_TRACE(ulTraceFlag);
	}

	CBitSetIter bsiterDisabled(*pbsDisabled);
	while (bsiterDisabled.FAdvance())
	{
		ULONG ulTraceFlag = bsiterDisabled.UlBit();
		GPOS_UNSET_TRACE(ulTraceFlag);
	}
}

// EOF
