//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColRefSetIterTest.cpp
//
//	@doc:
//		Test of ColRefSet iterator
//---------------------------------------------------------------------------

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"

#include "unittest/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "unittest/gpopt/base/CColRefSetIterTest.h"
#include "unittest/gpopt/translate/CTranslatorExprToDXLTest.h"

#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/CMDProviderMemory.h"

//---------------------------------------------------------------------------
//	@function:
//		CColRefSetIterTest::EresUnittest
//
//	@doc:
//		Unittest for bit vectors
//
//---------------------------------------------------------------------------
GPOS_RESULT
CColRefSetIterTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CColRefSetIterTest::EresUnittest_Basics)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefSetIterTest::EresUnittest_Basics
//
//	@doc:
//		Testing ctors/dtor; and pcr decoding;
//		Other functionality already tested in vanilla CBitSetIter;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CColRefSetIterTest::EresUnittest_Basics()
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
				NULL /* pceeval */,
				CTestUtils::Pcm(pmp)
				);

	// get column factory from optimizer context object
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	CWStringConst strName(GPOS_WSZ_LIT("Test Column"));
	CName name(&strName);

	// create a int4 datum
	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();


	ULONG ulCols = 10;
	for(ULONG i = 0; i < ulCols; i++)
	{
		CColRef *pcr = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
		pcrs->Include(pcr);

		GPOS_ASSERT(pcrs->FMember(pcr));
	}

	GPOS_ASSERT(pcrs->CElements() == ulCols);

	ULONG ulCount = 0;
	CColRefSetIter crsi(*pcrs);
	while(crsi.FAdvance())
	{
		GPOS_ASSERT((BOOL)crsi);

		CColRef *pcr = crsi.Pcr();
		GPOS_ASSERT(pcr->Name().FEquals(name));

		// to avoid unused variable warnings
		(void) pcr->UlId();

		ulCount++;
	}

	GPOS_ASSERT(ulCols == ulCount);
	GPOS_ASSERT(!((BOOL)crsi));

	pcrs->Release();

	return GPOS_OK;
}

// EOF
