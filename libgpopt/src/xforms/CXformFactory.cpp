//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformFactory.cpp
//
//	@doc:
//		Management of the global xform set
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpopt/xforms/xforms.h"

using namespace gpopt;

// global instance of xform factory
CXformFactory* CXformFactory::m_pxff = NULL;


//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::CXformFactory
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformFactory::CXformFactory
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_phmszxform(NULL),
	m_pxfsExploration(NULL),
	m_pxfsImplementation(NULL)
{
	GPOS_ASSERT(NULL != pmp);
	
	// null out array so dtor can be called prematurely
	for (ULONG i = 0; i < CXform::ExfSentinel; i++)
	{
		m_rgpxf[i] = NULL;
	}
	m_phmszxform = GPOS_NEW(pmp) HMSzXform(pmp);
	m_pxfsExploration = GPOS_NEW(pmp) CXformSet(pmp);
	m_pxfsImplementation = GPOS_NEW(pmp) CXformSet(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::~CXformFactory
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CXformFactory::~CXformFactory()
{
	GPOS_ASSERT(NULL == m_pxff &&
				"Xform factory has not been shut down");

	// delete all xforms in the array
	for (ULONG i = 0; i < CXform::ExfSentinel; i++)
	{
		if (NULL == m_rgpxf[i])
		{
			// dtor called after failing to populate array
			break;
		}
		
		m_rgpxf[i]->Release();
		m_rgpxf[i] = NULL;
	}

	m_phmszxform->Release();
	m_pxfsExploration->Release();
	m_pxfsImplementation->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Add
//
//	@doc:
//		Add a given xform to the array; enforce the order in which they
//		are added for readability/debugging
//
//---------------------------------------------------------------------------
void
CXformFactory::Add
	(
	CXform *pxform
	)
{	
	GPOS_ASSERT(NULL != pxform);
	CXform::EXformId exfid = pxform->Exfid();
	
	GPOS_ASSERT_IMP(0 < exfid, m_rgpxf[exfid - 1] != NULL &&
					"Incorrect order of instantiation");
	GPOS_ASSERT(NULL == m_rgpxf[exfid]);

	m_rgpxf[exfid] = pxform;

	// create name -> xform mapping
	ULONG ulLen = clib::UlStrLen(pxform->SzId());
	CHAR *szXformName = GPOS_NEW_ARRAY(m_pmp, CHAR, ulLen + 1);
	clib::SzStrNCpy(szXformName, pxform->SzId(), ulLen + 1);

#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
		m_phmszxform->FInsert(szXformName, pxform);
	GPOS_ASSERT(fInserted);

	CXformSet *pxfs = m_pxfsExploration;
	if (pxform->FImplementation())
	{
		pxfs = m_pxfsImplementation;
	}
#ifdef GPOS_DEBUG
	GPOS_ASSERT_IMP(pxform->FExploration(), pxfs == m_pxfsExploration);
	GPOS_ASSERT_IMP(pxform->FImplementation(), pxfs == m_pxfsImplementation);
	BOOL fSet =
#endif // GPOS_DEBUG
		pxfs->FExchangeSet(exfid);

	GPOS_ASSERT(!fSet);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Instantiate
//
//	@doc:
//		Construct all xforms
//
//---------------------------------------------------------------------------
void
CXformFactory::Instantiate()
{	
	Add(GPOS_NEW(m_pmp) CXformProject2ComputeScalar(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformExpandNAryJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformExpandNAryJoinMinCard(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformExpandNAryJoinDP(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformGet2TableScan(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformIndexGet2IndexScan(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformDynamicGet2DynamicTableScan(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformDynamicIndexGet2DynamicIndexScan(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementSequence(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementConstTableGet(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformUnnestTVF(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementTVF(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementTVFNoArgs(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSelect2Filter(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSelect2IndexGet(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSelect2DynamicIndexGet(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSelect2PartialDynamicIndexGet(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSimplifySelectWithSubquery(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSimplifyProjectWithSubquery(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSelect2Apply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformProject2Apply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformGbAgg2Apply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSubqJoin2Apply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSubqNAryJoin2Apply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoin2IndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoin2DynamicIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerApplyWithOuterKey2InnerJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoin2NLJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementIndexApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoin2HashJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerApply2InnerJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerApply2InnerJoinNoCorrelations(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementInnerCorrelatedApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuterApply2LeftOuterJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuterApply2LeftOuterJoinNoCorrelations(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementLeftOuterCorrelatedApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiApply2LeftSemiJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiApplyWithExternalCorrs2InnerJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiApply2LeftSemiJoinNoCorrelations(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiApply2LeftAntiSemiJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotInNoCorrelations(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformPushDownLeftOuterJoin (m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSimplifyLeftOuterJoin (m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuterJoin2NLJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuterJoin2HashJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiJoin2NLJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiJoin2HashJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiJoin2CrossProduct(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiJoinNotIn2CrossProduct(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiJoin2NLJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiJoinNotIn2NLJoinNotIn(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiJoin2HashJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftAntiSemiJoinNotIn2HashJoinNotIn(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformGbAgg2HashAgg(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformGbAgg2StreamAgg(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformGbAgg2ScalarAgg(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformGbAggDedup2HashAggDedup(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformGbAggDedup2StreamAggDedup(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementLimit(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformIntersectAll2LeftSemiJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformIntersect2Join(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformDifference2LeftAntiSemiJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformDifferenceAll2LeftAntiSemiJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformUnion2UnionAll(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementUnionAll(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInsert2DML(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformDelete2DML(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformUpdate2DML(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementDML(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementRowTrigger(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementSplit(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformJoinCommutativity(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformJoinAssociativity(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSemiJoinSemiJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSemiJoinAntiSemiJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSemiJoinAntiSemiJoinNotInSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSemiJoinInnerJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformAntiSemiJoinAntiSemiJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformAntiSemiJoinAntiSemiJoinNotInSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformAntiSemiJoinSemiJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformAntiSemiJoinInnerJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformAntiSemiJoinNotInAntiSemiJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformAntiSemiJoinNotInAntiSemiJoinNotInSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformAntiSemiJoinNotInSemiJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformAntiSemiJoinNotInInnerJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoinSemiJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoinAntiSemiJoinSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoinAntiSemiJoinNotInSwap(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiJoin2InnerJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiJoin2InnerJoinUnderGb(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiJoin2CrossProduct(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSplitLimit(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSimplifyGbAgg(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformCollapseGbAgg(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformPushGbBelowJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformPushGbDedupBelowJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformPushGbWithHavingBelowJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformPushGbBelowUnion(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformPushGbBelowUnionAll(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSplitGbAgg(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSplitGbAggDedup(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSplitDQA(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSequenceProject2Apply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementSequenceProject(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementAssert(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformCTEAnchor2Sequence(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformCTEAnchor2TrivialSelect(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInlineCTEConsumer(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInlineCTEConsumerUnderSelect(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementCTEProducer(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementCTEConsumer(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformExpandFullOuterJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformExternalGet2ExternalScan(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSelect2BitmapBoolOp(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformSelect2DynamicBitmapBoolOp(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementBitmapTableGet(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementDynamicBitmapTableGet(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoin2PartialDynamicIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementLeftSemiCorrelatedApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementLeftSemiCorrelatedApplyIn(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementLeftAntiSemiCorrelatedApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementLeftAntiSemiCorrelatedApplyNotIn(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiApplyIn2LeftSemiJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiApplyInWithExternalCorrs2InnerJoin(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoin2BitmapIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformImplementPartitionSelector(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformMaxOneRow2Assert(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoinWithInnerSelect2IndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoinWithInnerSelect2DynamicIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoin2DynamicBitmapIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoinWithInnerSelect2BitmapIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformGbAggWithMDQA2Join(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformCollapseProject(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformRemoveSubqDistinct(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuterJoin2BitmapIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuterJoin2IndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply(m_pmp));
	Add(GPOS_NEW(m_pmp) CXformLeftOuterJoinWithInnerSelect2IndexGetApply(m_pmp));

	GPOS_ASSERT(NULL != m_rgpxf[CXform::ExfSentinel - 1] &&
				"Not all xforms have been instantiated");
}

						  
//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Pxf
//
//	@doc:
//		Accessor of xform array
//
//---------------------------------------------------------------------------
CXform*
CXformFactory::Pxf
	(
	CXform::EXformId exfid
	)
	const
{
	CXform *pxf = m_rgpxf[exfid];
	GPOS_ASSERT(pxf->Exfid() == exfid);
	
	return pxf;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Pxf
//
//	@doc:
//		Accessor by xform name
//
//---------------------------------------------------------------------------
CXform *
CXformFactory::Pxf
	(
	const CHAR *szXformName
	)
	const
{
	return m_phmszxform->PtLookup(szXformName);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::EresInit
//
//	@doc:
//		Initializes global instance
//
//---------------------------------------------------------------------------
GPOS_RESULT
CXformFactory::EresInit()
{
	GPOS_ASSERT(NULL == Pxff() &&
			    "Xform factory was already initialized");

	GPOS_RESULT eres = GPOS_OK;

	// create xform factory memory pool
	IMemoryPool *pmp = CMemoryPoolManager::Pmpm()->PmpCreate(
		CMemoryPoolManager::EatTracker, true /*fThreadSafe*/, gpos::ullong_max);
	GPOS_TRY
	{
		// create xform factory instance
		m_pxff = GPOS_NEW(pmp) CXformFactory(pmp);
	}
	GPOS_CATCH_EX(ex)
	{
		// destroy memory pool if global instance was not created
		CMemoryPoolManager::Pmpm()->Destroy(pmp);
		m_pxff = NULL;

		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			eres = GPOS_OOM;
		}
		else
		{
			eres = GPOS_FAILED;
		}

		return eres;
	}
	GPOS_CATCH_END;

	// instantiating the factory
	m_pxff->Instantiate();

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Shutdown
//
//	@doc:
//		Cleans up allocated memory pool
//
//---------------------------------------------------------------------------
void
CXformFactory::Shutdown()
{
	CXformFactory *pxff = CXformFactory::Pxff();

	GPOS_ASSERT(NULL != pxff &&
			    "Xform factory has not been initialized");

	IMemoryPool *pmp = pxff->m_pmp;

	// destroy xform factory
	CXformFactory::m_pxff = NULL;
	GPOS_DELETE(pxff);

	// release allocated memory pool
	CMemoryPoolManager::Pmpm()->Destroy(pmp);
}


// EOF

