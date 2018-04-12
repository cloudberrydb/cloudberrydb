//---------------------------------------------------------------------------
//	Pivotal Software, Inc
//	Copyright (C) 2017 Pivotal Software, Inc
//---------------------------------------------------------------------------
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"

#include "unittest/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "unittest/gpopt/base/CEquivalenceClassesTest.h"
#include "unittest/gpopt/translate/CTranslatorExprToDXLTest.h"

#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/CMDProviderMemory.h"


// Unittest for bit vectors
GPOS_RESULT
CEquivalenceClassesTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CEquivalenceClassesTest::EresUnittest_NotDisjointEquivalanceClasses),
		GPOS_UNITTEST_FUNC(CEquivalenceClassesTest::EresUnittest_IntersectEquivalanceClasses)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

// Check disjoint equivalence classes are detected
GPOS_RESULT
CEquivalenceClassesTest::EresUnittest_NotDisjointEquivalanceClasses()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	// Setup an MD cache with a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache());
	mda.RegisterProvider(CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
				(
				pmp,
				&mda,
				NULL, /* pceeval */
				CTestUtils::Pcm(pmp)
				);

	// get column factory from optimizer context object
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	CWStringConst strName(GPOS_WSZ_LIT("Test Column"));
	CName name(&strName);

	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();

	ULONG ulCols = 10;
	for (ULONG i = 0; i < ulCols; i++)
	{
		CColRef *pcr = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
		pcrs->Include(pcr);

		GPOS_ASSERT(pcrs->FMember(pcr));
	}

	GPOS_ASSERT(pcrs->CElements() == ulCols);

	CColRefSet *pcrsTwo = GPOS_NEW(pmp) CColRefSet(pmp, *pcrs);
	GPOS_ASSERT(pcrsTwo->CElements() == ulCols);

	CColRefSet *pcrsThree = GPOS_NEW(pmp) CColRefSet(pmp);
	GPOS_ASSERT(pcrsThree->CElements() == 0);
	CColRef *pcrThree = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
	pcrsThree->Include(pcrThree);
	GPOS_ASSERT(pcrsThree->CElements() == 1);

	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	pcrs->AddRef();
	pcrsTwo->AddRef();
	pdrgpcrs->Append(pcrs);
	pdrgpcrs->Append(pcrsTwo);
	GPOS_ASSERT(!CUtils::FEquivalanceClassesDisjoint(pmp,pdrgpcrs));
	
	DrgPcrs *pdrgpcrsTwo = GPOS_NEW(pmp) DrgPcrs(pmp);
	pcrs->AddRef();
	pcrsThree->AddRef();
	pdrgpcrsTwo->Append(pcrs);
	pdrgpcrsTwo->Append(pcrsThree);
	GPOS_ASSERT(CUtils::FEquivalanceClassesDisjoint(pmp,pdrgpcrsTwo));
	
	pcrsThree->Release();
	pcrsTwo->Release();
	pcrs->Release();
	pdrgpcrs->Release();
	pdrgpcrsTwo->Release();

	return GPOS_OK;
}

// Check disjoint equivalence classes are detected
GPOS_RESULT
CEquivalenceClassesTest::EresUnittest_IntersectEquivalanceClasses()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	// Setup an MD cache with a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache());
	mda.RegisterProvider(CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
	(
	 pmp,
	 &mda,
	 NULL, /* pceeval */
	 CTestUtils::Pcm(pmp)
	 );

	// get column factory from optimizer context object
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	CWStringConst strName(GPOS_WSZ_LIT("Test Column"));
	CName name(&strName);

	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();

	ULONG ulCols = 10;
	for (ULONG i = 0; i < ulCols; i++)
	{
		CColRef *pcr = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
		pcrs->Include(pcr);

		GPOS_ASSERT(pcrs->FMember(pcr));
	}

	GPOS_ASSERT(pcrs->CElements() == ulCols);

	// Generate equivalence classes
	INT setBoundaryFirst[] = {2,5,7};
	DrgPcrs *pdrgpFirst = CTestUtils::createEquivalenceClasses(pmp, pcrs, setBoundaryFirst);

	INT setBoundarySecond[] = {1,4,5,6};
	DrgPcrs *pdrgpSecond = CTestUtils::createEquivalenceClasses(pmp, pcrs, setBoundarySecond);

	INT setBoundaryExpected[] = {1,2,4,5,6,7};
	DrgPcrs *pdrgpIntersectExpectedOp = CTestUtils::createEquivalenceClasses(pmp, pcrs, setBoundaryExpected);

	DrgPcrs *pdrgpResult = CUtils::PdrgpcrsIntersectEquivClasses(pmp, pdrgpFirst, pdrgpSecond);
	GPOS_ASSERT(CUtils::FEquivalanceClassesDisjoint(pmp,pdrgpResult));
	GPOS_ASSERT(CUtils::FEquivalanceClassesEqual(pmp, pdrgpResult, pdrgpIntersectExpectedOp));

	pcrs->Release();
	pdrgpFirst->Release();
	pdrgpResult->Release();
	pdrgpSecond->Release();
	pdrgpIntersectExpectedOp->Release();

	return GPOS_OK;
}
// EOF
