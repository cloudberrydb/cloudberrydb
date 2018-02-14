//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumTest.cpp
//
//	@doc:
//		Tests for datum classes
//---------------------------------------------------------------------------
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/base/CQueryContext.h"

#include "unittest/base.h"
#include "unittest/dxl/base/CDatumTest.h"
#include "unittest/gpopt/translate/CTranslatorExprToDXLTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumOidGPDB.h"
#include "naucrates/base/CDatumInt2GPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/base/CDatumInt8GPDB.h"
#include "naucrates/base/CDatumBoolGPDB.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"

#include "naucrates/dxl/gpdb_types.h"

//---------------------------------------------------------------------------
//	@function:
//		CDatumTest::EresUnittest
//
//	@doc:
//		Unittest for datum classes
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDatumTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CDatumTest::EresUnittest_Basics)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumTest::EresUnittest_Basics
//
//	@doc:
//		Basic datum tests; verify correct creation
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDatumTest::EresUnittest_Basics()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL, /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	typedef IDatum *(*Pfpdatum)(IMemoryPool*, BOOL);

	Pfpdatum rgpf[] =
		{
		PdatumInt2,
		PdatumInt4,
		PdatumInt8,
		PdatumBool,
		PdatumOid,
		PdatumGeneric,
		};
	
	BOOL rgf[] = {true, false};
	
	const ULONG ulFuncs = GPOS_ARRAY_SIZE(rgpf);
	const ULONG ulOptions = GPOS_ARRAY_SIZE(rgf);
	
	for (ULONG ul1 = 0; ul1 < ulFuncs; ul1++)
	{
		for (ULONG ul2 = 0; ul2 < ulOptions; ul2++)
		{
			CAutoTrace at(pmp);
			IOstream &os(at.Os());
			
			// generate datum
			BOOL fNull = rgf[ul2];
			IDatum *pdatum = rgpf[ul1](pmp, fNull);
			IDatum *pdatumCopy = pdatum->PdatumCopy(pmp);
			
			GPOS_ASSERT(pdatum->FMatch(pdatumCopy));
			
			const CWStringConst *pstrDatum = pdatum->Pstr(pmp);
			
	#ifdef GPOS_DEBUG
			os << std::endl;
			(void) pdatum->OsPrint(os);
			os << std::endl << pstrDatum->Wsz() << std::endl;
	#endif // GPOS_DEBUG
	
			os << "Datum type: " << pdatum->Eti() << std::endl;
	
			if (pdatum->FStatsMappable())
			{
				IDatumStatisticsMappable *pdatumMappable = (IDatumStatisticsMappable *) pdatum;
				
				if (pdatumMappable->FHasStatsLINTMapping())
				{
					os << "LINT stats value: " << pdatumMappable->LStatsMapping() << std::endl;
				}
	
				if (pdatumMappable->FHasStatsDoubleMapping())
				{
					os << "Double stats value: " << pdatumMappable->DStatsMapping() << std::endl;
				}
			}
			
			// cleanup
			pdatum->Release();
			pdatumCopy->Release();
			GPOS_DELETE(pstrDatum);
		}
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumTest::PdatumOid
//
//	@doc:
//		Create an oid datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumTest::PdatumOid
	(
	IMemoryPool *pmp,
	BOOL fNull
	)
{
	return GPOS_NEW(pmp) CDatumOidGPDB(CTestUtils::m_sysidDefault, 1 /*oVal*/, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumTest::PdatumInt2
//
//	@doc:
//		Create an int2 datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumTest::PdatumInt2
	(
	IMemoryPool *pmp,
	BOOL fNull
	)
{
	return GPOS_NEW(pmp) CDatumInt2GPDB(CTestUtils::m_sysidDefault, 1 /*sVal*/, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumTest::PdatumInt4
//
//	@doc:
//		Create an int4 datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumTest::PdatumInt4
	(
	IMemoryPool *pmp,
	BOOL fNull
	)
{
	return GPOS_NEW(pmp) CDatumInt4GPDB(CTestUtils::m_sysidDefault, 1 /*iVal*/, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumTest::PdatumInt8
//
//	@doc:
//		Create an int8 datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumTest::PdatumInt8
	(
	IMemoryPool *pmp,
	BOOL fNull
	)
{
	return GPOS_NEW(pmp) CDatumInt8GPDB(CTestUtils::m_sysidDefault, 1 /*lVal*/, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumTest::PdatumBool
//
//	@doc:
//		Create a bool datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumTest::PdatumBool
	(
	IMemoryPool *pmp,
	BOOL fNull
	)
{
	return GPOS_NEW(pmp) CDatumBoolGPDB(CTestUtils::m_sysidDefault, false /*fVal*/, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumTest::PdatumGeneric
//
//	@doc:
//		Create a generic datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumTest::PdatumGeneric
	(
	IMemoryPool *pmp,
	BOOL fNull
	)
{
	CMDIdGPDB *pmdidChar = GPOS_NEW(pmp) CMDIdGPDB(GPDB_CHAR);

	const CHAR *szVal = "test";
	return GPOS_NEW(pmp) CDatumGenericGPDB
							(
							pmp,
							pmdidChar,
							IDefaultTypeModifier,
							szVal,
							5 /*ulLength*/,
							fNull,
							0 /*lValue*/,
							0/*dValue*/
							);
}


// EOF
