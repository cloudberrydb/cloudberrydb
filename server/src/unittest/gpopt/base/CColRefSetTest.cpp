//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColRefSetTest.cpp
//
//	@doc:
//		Tests for CColRefSet
//---------------------------------------------------------------------------
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"

#include "unittest/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "unittest/gpopt/base/CColRefSetTest.h"
#include "unittest/gpopt/translate/CTranslatorExprToDXLTest.h"

#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/CMDProviderMemory.h"


//---------------------------------------------------------------------------
//	@function:
//		CColRefSetTest::EresUnittest
//
//	@doc:
//		Unittest for bit vectors
//
//---------------------------------------------------------------------------
GPOS_RESULT
CColRefSetTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CColRefSetTest::EresUnittest_Basics)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefSetTest::EresUnittest_Basics
//
//	@doc:
//		Very basic tests; setops tested in context of CBitSet already
//
//---------------------------------------------------------------------------
GPOS_RESULT
CColRefSetTest::EresUnittest_Basics()
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
	for(ULONG i = 0; i < ulCols; i++)
	{
		CColRef *pcr = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
		pcrs->Include(pcr);

		GPOS_ASSERT(pcrs->FMember(pcr));
	}

	GPOS_ASSERT(pcrs->CElements() == ulCols);

	CColRefSet *pcrsTwo = GPOS_NEW(pmp) CColRefSet(pmp, *pcrs);
	GPOS_ASSERT(pcrsTwo->CElements() == ulCols);

	pcrsTwo->Release();
	pcrs->Release();

	return GPOS_OK;
}


// EOF
