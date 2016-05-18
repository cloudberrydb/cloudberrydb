//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDAccessorTest.cpp
//
//	@doc:
//		Tests accessing objects from the metadata cache.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/error/CAutoTrace.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/io/COstreamString.h"

#include "gpos/memory/CCacheFactory.h"
#include "gpos/task/CAutoTaskProxy.h"


#include "naucrates/md/CMDProviderMemory.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeOid.h"
#include "naucrates/md/IMDTypeGeneric.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDColumn.h"
#include "naucrates/md/IMDCheckConstraint.h"
#include "naucrates/md/IMDPartConstraint.h"
#include "naucrates/md/IMDCast.h"
#include "naucrates/md/IMDScCmp.h"

#include "naucrates/exception.h"

#include "naucrates/base/IDatumInt4.h"
#include "naucrates/base/IDatumBool.h"
#include "naucrates/base/IDatumOid.h"

#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "unittest/base.h"
#include "unittest/gpopt/mdcache/CMDAccessorTest.h"
#include "unittest/gpopt/mdcache/CMDProviderTest.h"
#include "unittest/gpopt/CTestUtils.h"


#define GPOPT_MDCACHE_LOOKUP_THREADS	8
#define GPOPT_MDCACHE_MDAS	4

#define GPDB_AGG_AVG OID(2101)
#define GPDB_OP_INT4_LT OID(97)
#define GPDB_FUNC_TIMEOFDAY OID(274)

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest
//
//	@doc:
//		
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_Datum),
#ifdef GPOS_DEBUG
		GPOS_UNITTEST_FUNC_ASSERT(CMDAccessorTest::EresUnittest_DatumGeneric),
#endif
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_Navigate),
		GPOS_UNITTEST_FUNC_THROW
			(
			CMDAccessorTest::EresUnittest_Negative,
			gpdxl::ExmaMD,
			gpdxl::ExmiMDCacheEntryNotFound
			),
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_Indexes),
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_CheckConstraint),
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_IndexPartConstraint),
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_Cast),
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_ScCmp),
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_ConcurrentAccessSingleMDA),
		GPOS_UNITTEST_FUNC(CMDAccessorTest::EresUnittest_ConcurrentAccessMultipleMDA)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_Basic
//
//	@doc:
//		Test fetching metadata objects from a metadata cache
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_Basic()
{
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
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	// lookup different objects
	CMDIdGPDB *pmdidObject1 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID /* OID */, 1 /* major version */, 1 /* minor version */);
	CMDIdGPDB *pmdidObject2 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID /* OID */, 12 /* version */, 1 /* minor version */);

#ifdef GPOS_DEBUG
	const IMDRelation *pimdrel1 = 
#endif
	mda.Pmdrel(pmdidObject1);
	
#ifdef GPOS_DEBUG
	const IMDRelation *pimdrel2 = 
#endif	
	mda.Pmdrel(pmdidObject2);

	GPOS_ASSERT(pimdrel1->Pmdid()->FEquals(pmdidObject1) && pimdrel2->Pmdid()->FEquals(pmdidObject2));
	
	// access an object again
#ifdef GPOS_DEBUG
	const IMDRelation *pimdrel3 = 
#endif
	mda.Pmdrel(pmdidObject1);
	
	GPOS_ASSERT(pimdrel1 == pimdrel3);
	
	// access GPDB types, operators and aggregates
	CMDIdGPDB *pmdidType = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4, 1, 0);
	CMDIdGPDB *pmdidOp = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OP_INT4_LT, 1, 0);
	CMDIdGPDB *pmdidAgg = GPOS_NEW(pmp) CMDIdGPDB(GPDB_AGG_AVG, 1, 0);
	
#ifdef GPOS_DEBUG
	const IMDType *pimdtype = 
#endif
	mda.Pmdtype(pmdidType);
	
#ifdef GPOS_DEBUG
	const IMDScalarOp *pmdscop = 
#endif
	mda.Pmdscop(pmdidOp);

	GPOS_ASSERT(IMDType::EcmptL == pmdscop->Ecmpt());
	
#ifdef GPOS_DEBUG
	const IMDAggregate *pmdagg = 
#endif
	mda.Pmdagg(pmdidAgg);


	// access types by type info
#ifdef GPOS_DEBUG
	const IMDTypeInt4 *pmdtypeint4 = 
#endif
	mda.PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault);

#ifdef GPOS_DEBUG
	const IMDTypeBool *pmdtypebool = 
#endif
	mda.PtMDType<IMDTypeBool>(CTestUtils::m_sysidDefault);
	
	
#ifdef GPOS_DEBUG
	// for debug traces
	CAutoTrace at(pmp);
	IOstream &os(at.Os());

	// print objects
	os << std::endl;
	pimdrel1->DebugPrint(os);
	os << std::endl;

	pimdrel2->DebugPrint(os);
	os << std::endl;
	
	pimdtype->DebugPrint(os);
	os << std::endl;
		
	pmdscop->DebugPrint(os);
	os << std::endl;
			
	pmdagg->DebugPrint(os);
	os << std::endl;

	pmdtypeint4->DebugPrint(os);
	os << std::endl;
	
	pmdtypebool->DebugPrint(os);
	os << std::endl;
	
#endif	

	pmdidObject1->Release();
	pmdidObject2->Release();
	pmdidAgg->Release();
	pmdidOp->Release();
	pmdidType->Release();
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_Datum
//
//	@doc:
//		Test type factory method for creating base type datums
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_Datum()
{
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
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	// create an INT4 datum
	const IMDTypeInt4 *pmdtypeint4 = mda.PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault);
	IDatumInt4 *pdatumInt4 = pmdtypeint4->PdatumInt4(pmp, 5, false /* fNull */);
	GPOS_ASSERT(5 == pdatumInt4->IValue());

	// create a BOOL datum
	const IMDTypeBool *pmdtypebool = mda.PtMDType<IMDTypeBool>(CTestUtils::m_sysidDefault);
	IDatumBool *pdatumBool = pmdtypebool->PdatumBool(pmp, false, false /* fNull */);
	GPOS_ASSERT(false == pdatumBool->FValue());
	
	// create an OID datum
	const IMDTypeOid *pmdtypeoid = mda.PtMDType<IMDTypeOid>(CTestUtils::m_sysidDefault);
	IDatumOid *pdatumOid = pmdtypeoid->PdatumOid(pmp, 20, false /* fNull */);
	GPOS_ASSERT(20 == pdatumOid->OidValue());

	
#ifdef GPOS_DEBUG
	// for debug traces
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	
	// print objects
	oss << std::endl;
	oss << "Int4 datum" << std::endl;
	pdatumInt4->OsPrint(oss);
	
	oss << std::endl;
	oss << "Bool datum" << std::endl;
	pdatumBool->OsPrint(oss);
	
	oss << std::endl;
	oss << "Oid datum" << std::endl;
	pdatumOid->OsPrint(oss);

	GPOS_TRACE(str.Wsz());
#endif

	// cleanup
	pdatumInt4->Release();
	pdatumBool->Release();
	pdatumOid->Release();
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_DatumGeneric
//
//	@doc:
//		Test asserting during an attempt to obtain a generic type from the cache
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_DatumGeneric()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// Setup an MD cache with a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// attempt to obtain a generic type from the cache should assert
	(void) mda.PtMDType<IMDTypeGeneric>();

	return GPOS_FAILED;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_Navigate
//
//	@doc:
//		Test fetching a MD object from the cache and navigating its dependent
//		objects
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_Navigate()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();

	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);
	
	// lookup a function in the MD cache
	CMDIdGPDB *pmdidFunc = GPOS_NEW(pmp) CMDIdGPDB(GPDB_FUNC_TIMEOFDAY /* OID */, 1 /* major version */, 0 /* minor version */);

	const IMDFunction *pmdfunc = mda.Pmdfunc(pmdidFunc);
	
	// lookup function return type
	IMDId *pmdidFuncReturnType = pmdfunc->PmdidTypeResult();
	const IMDType *pimdtype = mda.Pmdtype(pmdidFuncReturnType);
		
	// lookup equality operator for function return type
	IMDId *pmdidEqOp = pimdtype->PmdidCmp(IMDType::EcmptEq);

#ifdef GPOS_DEBUG
	const IMDScalarOp *pmdscop = 
#endif
	mda.Pmdscop(pmdidEqOp);
		
#ifdef GPOS_DEBUG
	// print objects
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	
	oss << std::endl;
	oss << std::endl;

	pmdfunc->DebugPrint(oss);
	oss << std::endl;

	pimdtype->DebugPrint(oss);
	oss << std::endl;
	
	pmdscop->DebugPrint(oss);
	oss << std::endl;
					
	GPOS_TRACE(str.Wsz());
	
#endif
	
	pmdidFunc->Release();
	
	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_Indexes
//
//	@doc:
//		Test fetching indexes from the cache
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_Indexes()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	CAutoTrace at(pmp);

#ifdef GPOS_DEBUG
	IOstream &os(at.Os());
#endif // GPOS_DEBUG
	
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
	
	// lookup a relation in the MD cache
	CMDIdGPDB *pmdidRel =  GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1 /* major */, 1 /* minor version */);
	
	const IMDRelation *pmdrel = mda.Pmdrel(pmdidRel);
	
	GPOS_ASSERT(0 < pmdrel->UlIndices());
	
	IMDId *pmdidIndex = pmdrel->PmdidIndex(0);
	const IMDIndex *pmdindex = mda.Pmdindex(pmdidIndex);
	
	ULONG ulKeys = pmdindex->UlKeys();
	
#ifdef GPOS_DEBUG
	// print index
	pmdindex->DebugPrint(os);
	
	os << std::endl;
	os << "Index columns: " << std::endl;
		
#endif // GPOS_DEBUG

	for (ULONG ul = 0; ul < ulKeys; ul++)
	{
		ULONG ulKeyColumn = pmdindex->UlKey(ul);
		
#ifdef GPOS_DEBUG
		const IMDColumn *pmdcol = 
#endif // GPOS_DEBUG
		pmdrel->Pmdcol(ulKeyColumn);
		
#ifdef GPOS_DEBUG
		pmdcol->DebugPrint(os); 
#endif // GPOS_DEBUG	
	}
	
	pmdidRel->Release();
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_CheckConstraint
//
//	@doc:
//		Test fetching check constraint from the cache and translating the 
//		check constraint scalar expression
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_CheckConstraint()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CAutoTrace at(pmp);

#ifdef GPOS_DEBUG
	IOstream &os(at.Os());
#endif // GPOS_DEBUG

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
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	// lookup a relation in the MD cache
	CMDIdGPDB *pmdidRel =  GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID21, 1 /* major */, 1 /* minor version */);

	const IMDRelation *pmdrel = mda.Pmdrel(pmdidRel);
	GPOS_ASSERT(0 < pmdrel->UlCheckConstraints());

	// create the array of column reference for the table columns
	// for the DXL to Expr translation
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	const ULONG ulCols = pmdrel->UlColumns();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ul);
		const IMDType *pmdtype = mda.Pmdtype(pmdcol->PmdidType());
		CColRef *pcr = pcf->PcrCreate(pmdtype);
		pdrgpcr->Append(pcr);
	}

	// get one of its check constraint
	IMDId *pmdidCheckConstraint = pmdrel->PmdidCheckConstraint(0);
	const IMDCheckConstraint *pmdCheckConstraint = mda.Pmdcheckconstraint(pmdidCheckConstraint);

#ifdef GPOS_DEBUG
	os << std::endl;
	// print check constraint
	pmdCheckConstraint->DebugPrint(os);
	os << std::endl;
#endif // GPOS_DEBUG

	// extract and then print the check constraint expression	
	CExpression *pexpr = pmdCheckConstraint->Pexpr(pmp, &mda, pdrgpcr);

#ifdef GPOS_DEBUG
	pexpr->DbgPrint();
	os << std::endl;
#endif // GPOS_DEBUG

	// clean up
	pexpr->Release();
	pdrgpcr->Release();
	pmdidRel->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_IndexPartConstraint
//
//	@doc:
//		Test fetching part constraints for indexes on partitioned tables
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_IndexPartConstraint()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CAutoTrace at(pmp);

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
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	// lookup a relation in the MD cache
	CMDIdGPDB *pmdidRel =  GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID22);

	const IMDRelation *pmdrel = mda.Pmdrel(pmdidRel);
	GPOS_ASSERT(0 < pmdrel->UlIndices());

	// create the array of column reference for the table columns
	// for the DXL to Expr translation
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	const ULONG ulCols = pmdrel->UlColumns();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ul);

		const IMDType *pmdtype = mda.Pmdtype(pmdcol->PmdidType());
		CColRef *pcr = pcf->PcrCreate(pmdtype);
		pdrgpcr->Append(pcr);
	}

	// get one of its indexes
	GPOS_ASSERT(0 < pmdrel->UlIndices());
	IMDId *pmdidIndex = pmdrel->PmdidIndex(0);
	const IMDIndex *pmdindex = mda.Pmdindex(pmdidIndex);

	// extract and then print the part constraint expression
	IMDPartConstraint *pmdpartcnstr = pmdindex->Pmdpartcnstr();
	GPOS_ASSERT(NULL != pmdpartcnstr);
	
	CExpression *pexpr = pmdpartcnstr->Pexpr(pmp, &mda, pdrgpcr);

#ifdef GPOS_DEBUG
	IOstream &os(at.Os());

	pexpr->DbgPrint();
	os << std::endl;
#endif // GPOS_DEBUG

	// clean up
	pexpr->Release();
	pdrgpcr->Release();
	pmdidRel->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_Cast
//
//	@doc:
//		Test fetching cast objects from the MD cache
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_Cast()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CAutoTrace at(pmp);

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

	const IMDType *pmdtypeInt = mda.PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault);
	const IMDType *pmdtypeOid = mda.PtMDType<IMDTypeOid>(CTestUtils::m_sysidDefault);
	const IMDType *pmdtypeBigInt = mda.PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);

#ifdef GPOS_DEBUG
	const IMDCast *pmdcastInt2BigInt = 
#endif // GPOS_DEBUG
	mda.Pmdcast(pmdtypeInt->Pmdid(), pmdtypeBigInt->Pmdid());
	
	GPOS_ASSERT(!pmdcastInt2BigInt->FBinaryCoercible());
	GPOS_ASSERT(pmdcastInt2BigInt->PmdidCastFunc()->FValid());
	GPOS_ASSERT(pmdcastInt2BigInt->PmdidSrc()->FEquals(pmdtypeInt->Pmdid()));
	GPOS_ASSERT(pmdcastInt2BigInt->PmdidDest()->FEquals(pmdtypeBigInt->Pmdid()));
	
#ifdef GPOS_DEBUG
	const IMDCast *pmdcastInt2Oid = 
#endif // GPOS_DEBUG
	mda.Pmdcast(pmdtypeInt->Pmdid(), pmdtypeOid->Pmdid());
	
	GPOS_ASSERT(pmdcastInt2Oid->FBinaryCoercible());
	GPOS_ASSERT(!pmdcastInt2Oid->PmdidCastFunc()->FValid());
	
#ifdef GPOS_DEBUG
	const IMDCast *pmdcastOid2Int = 
#endif // GPOS_DEBUG
	mda.Pmdcast(pmdtypeOid->Pmdid(), pmdtypeInt->Pmdid());
	
	GPOS_ASSERT(pmdcastOid2Int->FBinaryCoercible());
	GPOS_ASSERT(!pmdcastOid2Int->PmdidCastFunc()->FValid());

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_ScCmp
//
//	@doc:
//		Test fetching scalar comparison objects from the MD cache
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_ScCmp()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CAutoTrace at(pmp);

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

	const IMDType *pmdtypeInt = mda.PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault);
	const IMDType *pmdtypeBigInt = mda.PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);

#ifdef GPOS_DEBUG
	const IMDScCmp *pmdScEqIntBigInt = 
#endif // GPOS_DEBUG
	mda.Pmdsccmp(pmdtypeInt->Pmdid(), pmdtypeBigInt->Pmdid(), IMDType::EcmptEq);
	
	GPOS_ASSERT(IMDType::EcmptEq == pmdScEqIntBigInt->Ecmpt());
	GPOS_ASSERT(pmdScEqIntBigInt->PmdidLeft()->FEquals(pmdtypeInt->Pmdid()));
	GPOS_ASSERT(pmdScEqIntBigInt->PmdidRight()->FEquals(pmdtypeBigInt->Pmdid()));

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_Negative
//
//	@doc:
//		Test fetching non-existing metadata objects from the MD cache
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_Negative()
{
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();
	
	// Setup an MD cache with a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);
	
	// lookup a non-existing objects
	CMDIdGPDB *pmdidNonExistingObject = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID /* OID */, 15 /* version */, 1 /* minor version */);

	// call should result in an exception
	(void) mda.Pmdrel(pmdidNonExistingObject);

	pmdidNonExistingObject->Release();
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_ConcurrentAccessSingleMDA
//
//	@doc:
//		Test concurrent access through a single MD accessor from different threads
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_ConcurrentAccessSingleMDA()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	
	// task memory pool
	CAutoMemoryPool ampTask;
	IMemoryPool *pmpTask = ampTask.Pmp();
	
	// setup a file-based provider
	// Setup an MD cache with a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// task parameters
	SMDCacheTaskParams mdtaskparams(pmpTask, &mda);

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
	
		CTask *rgPtsk[GPOPT_MDCACHE_LOOKUP_THREADS];
	
		TaskFuncPtr rgPfuncTask[] =
			{
			CMDAccessorTest::PvLookupSingleObj,
			CMDAccessorTest::PvLookupMultipleObj
			};
	
		const ULONG ulNumberOfTaskTypes = GPOS_ARRAY_SIZE(rgPfuncTask);
	
		// create tasks
		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
		{
			rgPtsk[i] = atp.PtskCreate
						(
						rgPfuncTask[i % ulNumberOfTaskTypes],
						&mdtaskparams
						);
	
			GPOS_CHECK_ABORT;
		}
			
		for (ULONG i = 0; i < GPOPT_MDCACHE_LOOKUP_THREADS; i++)
		{
			atp.Schedule(rgPtsk[i]);
		}
			
		GPOS_CHECK_ABORT;

		// wait for completion
		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
		{
			atp.Wait(rgPtsk[i]);
			GPOS_CHECK_ABORT;
	
		}
	}
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::EresUnittest_ConcurrentAccessMultipleMDA
//
//	@doc:
//		Test concurrent access through multiple MD accessors from different threads
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDAccessorTest::EresUnittest_ConcurrentAccessMultipleMDA()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

#ifdef GPOS_DEBUG
	BOOL fOld = IWorker::m_fEnforceTimeSlices;
	IWorker::m_fEnforceTimeSlices = false;
#endif

	GPOS_TRY
	{	
		CAutoTaskProxy atp(pmp, pwpm);
	
		// create multiple tasks eash of which will init a separate MD accessor and use it to
		// lookup objects from the respective accessor using different threads
		CTask *rgPtsk[GPOPT_MDCACHE_MDAS];
		
		// create tasks
		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
		{
			rgPtsk[i] = atp.PtskCreate
						(
						PvInitMDAAndLookup,
						CMDCache::Pcache()	// task arg
						);
	
			GPOS_CHECK_ABORT;
		}
			
		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
		{
			atp.Schedule(rgPtsk[i]);
		}
			
		GPOS_CHECK_ABORT;

		// wait for completion
		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
		{
			atp.Wait(rgPtsk[i]);
			GPOS_CHECK_ABORT;
	
		}
	}
	GPOS_CATCH_EX(ex)
	{
#ifdef GPOS_DEBUG
		IWorker::m_fEnforceTimeSlices = fOld;
#endif

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;
	
#ifdef GPOS_DEBUG
	IWorker::m_fEnforceTimeSlices = fOld;
#endif

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::PvLookupSingleObj
//
//	@doc:
//		A task that looks up a single object from the MD cache
//
//---------------------------------------------------------------------------
void *
CMDAccessorTest::PvLookupSingleObj
	(
	 void * pv
	)
{
	GPOS_CHECK_ABORT;

	GPOS_ASSERT(NULL != pv);
	
	SMDCacheTaskParams *pmdtaskparams = (SMDCacheTaskParams *) pv;
	
	CMDAccessor *pmda = pmdtaskparams->m_pmda;

	IMemoryPool *pmp = pmdtaskparams->m_pmp;
	
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pmda);
	
	// lookup a cache object
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID /* OID */, 1 /* major version */, 1 /* minor version */);

	// lookup object
	(void) pmda->Pmdrel(pmdid);
	pmdid->Release();
	
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::PvLookupMultipleObj
//
//	@doc:
//		A task that looks up multiple objects from the MD cache
//
//---------------------------------------------------------------------------
void *
CMDAccessorTest::PvLookupMultipleObj
	(
	 void * pv
	)
{
	GPOS_CHECK_ABORT;

	GPOS_ASSERT(NULL != pv);

	SMDCacheTaskParams *pmdtaskparams = (SMDCacheTaskParams *) pv;
	
	CMDAccessor *pmda = pmdtaskparams->m_pmda;
	
	GPOS_ASSERT(NULL != pmda);
		
	// lookup cache objects
	const ULONG ulNumberOfObjects = 10;
	
	for (ULONG ul = 0; ul < ulNumberOfObjects; ul++)
	{
		GPOS_CHECK_ABORT;

		// lookup relation
		CMDIdGPDB *pmdid = GPOS_NEW(pmdtaskparams->m_pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID /*OID*/, 1 /*major*/, ul + 1 /*minor*/);
		(void) pmda->Pmdrel(pmdid);
		pmdid->Release();
	}
	
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessorTest::PvInitMDAAndLookup
//
//	@doc:
//		Task which initializes a MD accessor and looks up multiple objects through
//		that accessor
//
//---------------------------------------------------------------------------
void *
CMDAccessorTest::PvInitMDAAndLookup
	(
	void * pv
	)
{
	GPOS_ASSERT(NULL != pv);
	
	CMDAccessor::MDCache *pcache = (CMDAccessor::MDCache *) pv;

	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	
	// task memory pool
	CAutoMemoryPool ampTask;
	IMemoryPool *pmpTask = ampTask.Pmp();
	
	// scope for MD accessor
	{			
		// Setup an MD cache with a file-based provider
		CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
		pmdp->AddRef();
		CMDAccessor mda(pmp, pcache, CTestUtils::m_sysidDefault, pmdp);
		
		// task parameters
		SMDCacheTaskParams mdtaskparams(pmpTask, &mda);

		// scope for ATP
		{	
			CAutoTaskProxy atp(pmp, pwpm);
	
			CTask *rgPtsk[GPOPT_MDCACHE_LOOKUP_THREADS];
	
			TaskFuncPtr rgPfuncTask[] =
				{
				CMDAccessorTest::PvLookupSingleObj,
				CMDAccessorTest::PvLookupMultipleObj
				};
	
			const ULONG ulNumberOfTaskTypes = GPOS_ARRAY_SIZE(rgPfuncTask);
	
			// create tasks
			for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
			{
				rgPtsk[i] = atp.PtskCreate
							(
							rgPfuncTask[i % ulNumberOfTaskTypes],
							&mdtaskparams
							);
	
				GPOS_CHECK_ABORT;
			}
			
			for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
			{	
				atp.Schedule(rgPtsk[i]);
			}
			
			GPOS_CHECK_ABORT;
	
			// wait for completion
			for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
			{
				atp.Wait(rgPtsk[i]);
				GPOS_CHECK_ABORT;
	
			}
		}
	
	}
	
	return NULL;
}

// EOF
