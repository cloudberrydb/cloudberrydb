//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC Corp.
//
//	@filename:
//		CKeyCollectionTest.cpp
//
//	@doc:
//		Tests for CKeyCollectionTest
//---------------------------------------------------------------------------
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"

#include "unittest/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "unittest/gpopt/base/CKeyCollectionTest.h"

#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/CMDProviderMemory.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollectionTest::EresUnittest
//
//	@doc:
//		Unittest for key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CKeyCollectionTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CKeyCollectionTest::EresUnittest_Basics),
		GPOS_UNITTEST_FUNC(CKeyCollectionTest::EresUnittest_Subsumes)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollectionTest::EresUnittest_Basics
//
//	@doc:
//		Basic test for key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CKeyCollectionTest::EresUnittest_Basics()
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
	
	// create test set
	CWStringConst strName(GPOS_WSZ_LIT("Test Column"));
	CName name(&strName);
	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();

	CKeyCollection *pkc = GPOS_NEW(pmp) CKeyCollection(pmp);

	const ULONG ulCols = 10;
	for(ULONG i = 0; i < ulCols; i++)
	{
		CColRef *pcr = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
		pcrs->Include(pcr);
	}

	pkc->Add(pcrs);	
	GPOS_ASSERT(pkc->FKey(pcrs));
	
	DrgPcr *pdrgpcr = pkc->PdrgpcrKey(pmp);
	GPOS_ASSERT(pkc->FKey(pmp, pdrgpcr));
	
	pcrs->Include(pdrgpcr);
	GPOS_ASSERT(pkc->FKey(pcrs));

	pdrgpcr->Release();
	
	pkc->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollectionTest::EresUnittest_Subsumes
//
//	@doc:
//		Basic test for trimming key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CKeyCollectionTest::EresUnittest_Subsumes()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

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

	// create test set
	CWStringConst strName(GPOS_WSZ_LIT("Test Column"));
	CName name(&strName);
	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();

	CKeyCollection *pkc = GPOS_NEW(pmp) CKeyCollection(pmp);

	CColRefSet *pcrs0 = GPOS_NEW(pmp) CColRefSet(pmp);
	CColRefSet *pcrs1 = GPOS_NEW(pmp) CColRefSet(pmp);
	CColRefSet *pcrs2 = GPOS_NEW(pmp) CColRefSet(pmp);

	const ULONG ulCols = 10;
	const ULONG ulLen1 = 3;
	for(ULONG ul = 0; ul < ulCols; ul++)
	{
		CColRef *pcr = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
		pcrs0->Include(pcr);

		if (ul < ulLen1)
		{
			pcrs1->Include(pcr);
		}

		if (ul == 0)
		{
			pcrs2->Include(pcr);
		}
	}

	pkc->Add(pcrs0);
	pkc->Add(pcrs1);
	pkc->Add(pcrs2);

	GPOS_ASSERT(pkc->FKey(pcrs2));

	// get the second key
	DrgPcr *pdrgpcr = pkc->PdrgpcrKey(pmp, 1);
	GPOS_ASSERT(ulLen1 == pdrgpcr->UlLength());
	GPOS_ASSERT(pkc->FKey(pmp, pdrgpcr));

	// get the subsumed key
	DrgPcr *pdrgpcrSubsumed = pkc->PdrgpcrTrim(pmp, pdrgpcr);
	GPOS_ASSERT(pdrgpcr->UlLength() >= pdrgpcrSubsumed->UlLength());

	CColRefSet *pcrsSubsumed = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsSubsumed->Include(pdrgpcr);

#ifdef GPOS_DEBUG
	const ULONG ulLenSubsumed = pdrgpcrSubsumed->UlLength();
	for (ULONG ul = 0; ul < ulLenSubsumed; ul++)
	{
		CColRef *pcr = (*pdrgpcrSubsumed)[ul];
		GPOS_ASSERT(pcrsSubsumed->FMember(pcr));
	}
#endif // GPOS_DEBUG

	pcrsSubsumed->Release();
	pdrgpcr->Release();
	pdrgpcrSubsumed->Release();
	pkc->Release();

	return GPOS_OK;
}
// EOF
