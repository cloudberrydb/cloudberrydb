//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CIndexDescriptorTest.cpp
//
//	@doc:
//		Test for CIndexDescriptor
//---------------------------------------------------------------------------
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"

#include "unittest/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "unittest/gpopt/metadata/CIndexDescriptorTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/CMDProviderMemory.h"


//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptorTest::EresUnittest
//
//	@doc:
//		Unittest for metadata names
//
//---------------------------------------------------------------------------
GPOS_RESULT
CIndexDescriptorTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CIndexDescriptorTest::EresUnittest_Basic)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptorTest::EresUnittest_Basic
//
//	@doc:
//		Basic naming, key columns and index columns printing test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CIndexDescriptorTest::EresUnittest_Basic()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// Setup an MD cache with a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	CWStringConst strName(GPOS_WSZ_LIT("MyTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1);
	CTableDescriptor *ptabdesc = CTestUtils::PtabdescCreate(pmp, 10, pmdid, CName(&strName));

	// get the index associated with the table
	const IMDRelation *pmdrel = mda.Pmdrel(ptabdesc->Pmdid());
	GPOS_ASSERT(0 < pmdrel->UlIndices());

	// create an index descriptor
	IMDId *pmdidIndex = pmdrel->PmdidIndex(0); // get the first index
	const IMDIndex *pmdindex = mda.Pmdindex(pmdidIndex);
	CIndexDescriptor *pindexdesc  = CIndexDescriptor::Pindexdesc(pmp, ptabdesc, pmdindex);

#ifdef GPOS_DEBUG
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	pindexdesc->OsPrint(oss);

	GPOS_TRACE(str.Wsz());
#endif // GPOS_DEBUG

	// clean up
	ptabdesc->Release();
	pindexdesc->Release();

	return GPOS_OK;
}

// EOF
