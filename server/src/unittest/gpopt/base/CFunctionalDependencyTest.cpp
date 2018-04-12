//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CFunctionalDependencyTest.cpp
//
//	@doc:
//		Tests for functional dependencies
//---------------------------------------------------------------------------
#include "gpopt/base/CFunctionalDependency.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"

#include "unittest/base.h"
#include "unittest/gpopt/base/CFunctionalDependencyTest.h"
#include "unittest/gpopt/CTestUtils.h"


//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependencyTest::EresUnittest
//
//	@doc:
//		Unittest for functional dependencies
//
//---------------------------------------------------------------------------
GPOS_RESULT
CFunctionalDependencyTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CFunctionalDependencyTest::EresUnittest_Basics)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionalDependencyTest::EresUnittest_Basics
//
//	@doc:
//		Basic test for functional dependencies
//
//---------------------------------------------------------------------------
GPOS_RESULT
CFunctionalDependencyTest::EresUnittest_Basics()
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

	CWStringConst strName(GPOS_WSZ_LIT("Test Column"));
	CName name(&strName);

	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>();

	const ULONG ulCols = 3;
	CColRefSet *pcrsLeft = GPOS_NEW(pmp) CColRefSet(pmp);
	CColRefSet *pcrsRight = GPOS_NEW(pmp) CColRefSet(pmp);
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColRef *pcr = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
		pcrsLeft->Include(pcr);

		pcr = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation, name);
		pcrsRight->Include(pcr);
	}

	pcrsLeft->AddRef();
	pcrsRight->AddRef();
	CFunctionalDependency *pfdFst = GPOS_NEW(pmp) CFunctionalDependency(pcrsLeft, pcrsRight);

	pcrsLeft->AddRef();
	pcrsRight->AddRef();
	CFunctionalDependency *pfdSnd = GPOS_NEW(pmp) CFunctionalDependency(pcrsLeft, pcrsRight);

	GPOS_ASSERT(pfdFst->FEqual(pfdSnd));
	GPOS_ASSERT(pfdFst->UlHash() == pfdSnd->UlHash());

	 DrgPfd *pdrgpfd = GPOS_NEW(pmp) DrgPfd(pmp);
	 pfdFst->AddRef();
	 pdrgpfd->Append(pfdFst);
	 pfdSnd->AddRef();
	 pdrgpfd->Append(pfdSnd);
	 GPOS_ASSERT(CFunctionalDependency::FEqual(pdrgpfd, pdrgpfd));

	 DrgPcr *pdrgpcr = CFunctionalDependency::PdrgpcrKeys(pmp, pdrgpfd);
	 CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	 pcrs->Include(pdrgpcr);
	 CColRefSet *pcrsKeys = CFunctionalDependency::PcrsKeys(pmp, pdrgpfd);

	 GPOS_ASSERT(pcrsLeft->FEqual(pcrs));
	 GPOS_ASSERT(pcrsKeys->FEqual(pcrs));

	CAutoTrace at(pmp);
	at.Os()
		<< "FD1:" << *pfdFst << std::endl
		<< "FD2:" << *pfdSnd << std::endl;

	pfdFst->Release();
	pfdSnd->Release();
	pcrsLeft->Release();
	pcrsRight->Release();
	pdrgpfd->Release();
	pdrgpcr->Release();
	pcrs->Release();
	pcrsKeys->Release();

	return GPOS_OK;
}


// EOF
