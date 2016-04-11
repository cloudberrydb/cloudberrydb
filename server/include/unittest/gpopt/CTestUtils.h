//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTestUtils.h
//
//	@doc:
//		Optimizer test utility functions
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CTestUtils_H
#define GPOPT_CTestUtils_H

#include "gpos/base.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarSubquery.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CSystemId.h"

#include "gpdbcost/CCostModelGPDBLegacy.h"

#include "unittest/base.h"

#include "naucrates/statistics/CPoint.h"


#define GPOPT_MDCACHE_TEST_OID	1234
#define GPOPT_MDCACHE_TEST_OID_PARTITIONED 5678
#define GPOPT_MDCACHE_TEST_OID_PARTITIONED_WITH_INDEXES 5369655
#define GPOPT_TEST_REL_WIDTH	3  // number of columns in generated test relations
#define GPOPT_TEST_PART_INDEX 1234567

#define GPOPT_TEST_REL_OID1	OID(11111)
#define GPOPT_TEST_REL_OID2	OID(22222)
#define GPOPT_TEST_REL_OID3	OID(33333)
#define GPOPT_TEST_REL_OID4	OID(44444)
#define GPOPT_TEST_REL_OID5	OID(55555)
#define GPOPT_TEST_REL_OID6	OID(66666)
#define GPOPT_TEST_REL_OID7	OID(77777)
#define GPOPT_TEST_REL_OID8	OID(88888)
#define GPOPT_TEST_REL_OID9	OID(99999)
#define GPOPT_TEST_REL_OID10 OID(10000)
#define GPOPT_TEST_REL_OID11 OID(10111)
#define GPOPT_TEST_REL_OID12 OID(10222)
#define GPOPT_TEST_REL_OID13 OID(10333)
#define GPOPT_TEST_REL_OID14 OID(10444)
#define GPOPT_TEST_REL_OID15 OID(10555)
#define GPOPT_TEST_REL_OID16 OID(10666)
#define GPOPT_TEST_REL_OID17 OID(10777)
#define GPOPT_TEST_REL_OID18 OID(10888)
#define GPOPT_TEST_REL_OID19 OID(10999)
#define GPOPT_TEST_REL_OID20 OID(10200)
#define GPOPT_TEST_REL_OID21 OID(10311)
#define GPOPT_TEST_REL_OID22 OID(27118)

#define GPDB_INT4_LT_OP OID(97)
#define GPDB_INT4_EQ_OP OID(96)
#define GPDB_INT4_NEQ_OP OID(518)
#define GPDB_INT4_SUM_AGG OID(2114)
#define GPDB_INT8_GENERATE_SERIES OID(1067)

// GPDB window functions
#define GPDB_WIN_ROW_NUMBER OID(7000)
#define GPDB_WIN_RANK OID(7001)
#define GPDB_WIN_FIRST_VALUE OID(7029)
#define GPDB_WIN_LAST_VALUE OID(7012)

// forward declarations
namespace gpmd
{
	class CMDIdGPDB;
}

namespace gpopt
{
	using namespace gpos;
	using gpmd::CMDIdGPDB;

	// forward declarations
	class CConstraintInterval;
	class IConstExprEvaluator;

	typedef CDynamicPtrArray<CExpression, CleanupNULL> DrgPexprJoins;

	typedef BOOL (FnDXLPlanChecker)(CDXLNode *);

	//---------------------------------------------------------------------------
	//	@class:
	//		CTestUtils
	//
	//	@doc:
	//		Test utility functions
	//
	//---------------------------------------------------------------------------
	class CTestUtils
	{

		private:

			// local memory pool
			static
			IMemoryPool *m_pmp;

			// construct an array of segment ids
			static gpdxl::DrgPi *PdrgpiSegments(IMemoryPool *pmp);

			// generate minidump file name from passed file name
			static
			CHAR *SzMinidumpFileName(IMemoryPool *pmp, const CHAR *szFileName);

			// return the number of segments, default return GPOPT_TEST_SEGMENTS
			static
			ULONG UlSegments(COptimizerConfig *poconf);

		public:

			// pair of DXL query file and the corresponding DXL plan file
			struct STestCase
			{
				const CHAR *szInputFile;
				const CHAR *szOutputFile;
			};

			//---------------------------------------------------------------------------
			//	@class:
			//		CTestUtils
			//
			//	@doc:
			//		Set up basic structures needed for most tests
			//
			//---------------------------------------------------------------------------
			class CTestSetup
			{
				private:
					// memory pool
					CAutoMemoryPool m_amp;

					// metadata accessor
					CMDAccessor m_mda;

					// optimization context
					CAutoOptCtxt m_aoc;

					// private ctor
					CTestSetup(const CTestSetup &);

					// set up a file based provider
					static CMDProviderMemory *PmdpSetupFileBasedProvider();
				public:
					// ctor
					CTestSetup();

					// return the memory pool
					IMemoryPool *Pmp()
					{
						return m_amp.Pmp();
					}

					// return the metadata acccessor
					CMDAccessor *Pmda()
					{
						return &m_mda;
					}
			};


			//-------------------------------------------------------------------
			// Initialization/Destruction of provider file
			//-------------------------------------------------------------------
			// initialize provider file
			static
			void InitProviderFile(IMemoryPool *pmp);

			// destroy metadata provider
			static
			void DestroyMDProvider();

			//-------------------------------------------------------------------
			// Helpers for generating expressions
			//-------------------------------------------------------------------

			// generate a plain table descriptor
			static
			CTableDescriptor *PtabdescPlain(IMemoryPool *pmp, ULONG ulCols, IMDId *pmdid, const CName &nameTable, BOOL fNullable = false);

			// generate a plain table descriptor, where the column names are generated using a format string containing %d
			static
			CTableDescriptor *PtabdescPlainWithColNameFormat(IMemoryPool *pmp, ULONG ulCols, IMDId *pmdid, const WCHAR *wszColNameFormat, const CName &nameTable, BOOL fNullable = false);

			// generate a table descriptor
			static
			CTableDescriptor *PtabdescCreate(IMemoryPool *pmp, ULONG ulCols, IMDId *pmdid, const CName &name, BOOL fPartitioned = false);

			// generate a get expression
			static
			CExpression *PexprLogicalGet(IMemoryPool *pmp, CTableDescriptor *ptabdesc, const CWStringConst *pstrTableAlias);

			// generate a get expression over table with nullable columns
			static
			CExpression *PexprLogicalGetNullable(IMemoryPool *pmp, OID oidTable, const CWStringConst *pstrTableName, const CWStringConst *pstrTableAlias);

			// generate a get expression
			static
			CExpression *PexprLogicalGet(IMemoryPool *pmp, CWStringConst *pstrTableName, CWStringConst *pstrTableAlias, ULONG ulTableId);

			// generate a random get expression
			static
			CExpression *PexprLogicalGet(IMemoryPool *pmp);

			// generate a random external get expression
			static
			CExpression *PexprLogicalExternalGet(IMemoryPool *pmp);

			// generate a random assert expression
			static
			CExpression *PexprLogicalAssert(IMemoryPool *pmp);
			
			// generate a random get expression on a partitioned table
			static
			CExpression *PexprLogicalGetPartitioned(IMemoryPool *pmp);

			// generate a randomized get expression for a partitioned table with indexes
			static
			CExpression *PexprLogicalDynamicGetWithIndexes(IMemoryPool *pmp);

			// generate a select expression with random equality predicate
			static
			CExpression *PexprLogicalSelect(IMemoryPool *pmp, CExpression *pexpr);

			// generate a select expression
			static
			CExpression *PexprLogicalSelect(IMemoryPool *pmp, CWStringConst *pstrTableName, CWStringConst *pstrTableAlias, ULONG ulTableId);

			// generate a random select expression
			static
			CExpression *PexprLogicalSelect(IMemoryPool *pmp);

			// generate a select expression with a contradiction
			static
			CExpression *PexprLogicalSelectWithContradiction(IMemoryPool *pmp);

			// generate a select on top of a partitioned table get with a filter on the part key
			static
			CExpression *PexprLogicalSelectPartitioned(IMemoryPool *pmp);

			// generate a project element with sum agg function
			static
			CExpression *PexprPrjElemWithSum(IMemoryPool *pmp, CColRef *pcr);

			// generate an join of specific join type on  get with a predicate 
			// on the part key with a partitioned table on the outer side 
			template<class T>
			static
			CExpression *PexprJoinPartitionedOuter(IMemoryPool *pmp);
			
			// generate a join of specific join type on  get with a predicate 
			// on the part key with a partitioned table on the inner side 
			template<class T>
			static
			CExpression *PexprJoinPartitionedInner(IMemoryPool *pmp);
			
			// generate an join of specific join type on top of a partitioned 
			// table get with a predicate on the part key
			template<class T>
			static
			CExpression *PexprJoinPartitioned(IMemoryPool *pmp, BOOL fOuterPartitioned);
			
			// generate an 3-way join including a partitioned table
			static
			CExpression *Pexpr3WayJoinPartitioned(IMemoryPool *pmp);

			// generate an 4-way join including a partitioned table
			static
			CExpression *Pexpr4WayJoinPartitioned(IMemoryPool *pmp);

			// generate a random select expression with nested AND tree
			static
			CExpression *PexprLogicalSelectWithNestedAnd(IMemoryPool *pmp);

			// generate a random select expression with nested OR tree
			static
			CExpression *PexprLogicalSelectWithNestedOr(IMemoryPool *pmp);

			// generate a random select expression with an even number of nested NOT nodes
			static
			CExpression *PexprLogicalSelectWithEvenNestedNot(IMemoryPool *pmp);

			// generate a random select expression with an odd number of nested NOT nodes
			static
			CExpression *PexprLogicalSelectWithOddNestedNot(IMemoryPool *pmp);

			// generate a random select expression with nested AND-OR tree
			static
			CExpression *PexprLogicalSelectWithNestedAndOrNot(IMemoryPool *pmp);

			// generate a Select expression with ANY/ALL subquery predicate over a const table get
			static
			CExpression *PexprLogicalSubqueryWithConstTableGet(IMemoryPool *pmp, COperator::EOperatorId eopid);

			// generate a random select expression with constant ANY subquery
			static
			CExpression *PexprLogicalSelectWithConstAnySubquery(IMemoryPool *pmp);

			// generate a random select expression with constant ALL subquery
			static
			CExpression *PexprLogicalSelectWithConstAllSubquery(IMemoryPool *pmp);

			// generate a correlated select expression
			static
			CExpression *PexprLogicalSelectCorrelated(IMemoryPool *pmp);

			// generate a correlated select expression
			static
			CExpression *PexprLogicalSelectCorrelated(IMemoryPool *pmp, CColRefSet *pcrsOuter, ULONG ulLevel);

			// generate a select on top of outer join expression
			static
			CExpression *PexprLogicalSelectOnOuterJoin(IMemoryPool *pmp);

			// generate a join expression with a random equality predicate
			template<class T>
			static
			CExpression *PexprLogicalJoin(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// generate a random binary join expression of specific join type
			template<class T>
			static
			CExpression *PexprLogicalJoin(IMemoryPool *pmp);

			// generate join with correlation beneath it
			static
			CExpression *PexprLogicalJoinCorrelated(IMemoryPool *pmp);
	
			// generate join with a partitioned and indexed inner child
			static
			CExpression *PexprLogicalJoinWithPartitionedAndIndexedInnerChild(IMemoryPool *pmp);
			
			// generate a select expression with predicate between a scalar ident and a constant
			static
			CExpression *PexprLogicalSelectCmpToConst(IMemoryPool *pmp);

			// generate a select expression with an array compare
			static
			CExpression *PexprLogicalSelectArrayCmp(IMemoryPool *pmp);
			
			// generate an n-ary join expression
			static
			CExpression *PexprLogicalNAryJoin(IMemoryPool *pmp, DrgPexpr *pdrgpexpr);

			// generate an n-ary join expression using given array of relation names
			static
			CExpression *PexprLogicalNAryJoin(IMemoryPool *pmp, CWStringConst *pstrRel, ULONG *pulRel, ULONG ulRels, BOOL fCrossProduct);

			// generate a random n-ary expression
			static
			CExpression *PexprLogicalNAryJoin(IMemoryPool *pmp);

			// generate left outer join on top of n-ary join expression
			static
			CExpression *PexprLeftOuterJoinOnNAryJoin(IMemoryPool *pmp);

			// generate n-ary join on top of left outer join expression
			static
			CExpression *PexprNAryJoinOnLeftOuterJoin(IMemoryPool *pmp);

			// generate a project expression
			static
			CExpression *PexprLogicalProject(IMemoryPool *pmp, CExpression *pexpr, CColRef *pcr, CColRef *pcrNew);

			// generate a random project expression
			static
			CExpression *PexprLogicalProject(IMemoryPool *pmp);

			// generate a Project over GbAgg with correlation beneath it
			static
			CExpression *PexprLogicalProjectGbAggCorrelated(IMemoryPool *pmp);

			// generate a limit expression
			static
			CExpression *PexprLogicalLimit(IMemoryPool *pmp, CExpression *pexpr, LINT iStart, LINT iRows, BOOL fGlobal, BOOL fHasCount);

			// generate a random limit expression
			static
			CExpression *PexprLogicalLimit(IMemoryPool *pmp);

			// generate a random group by expression
			static
			CExpression *PexprLogicalGbAgg(IMemoryPool *pmp);

			// generate a random group by expression
			static
			CExpression *PexprLogicalGbAggWithInput(IMemoryPool *pmp, CExpression *pexprInput);

			// generate a random group by with a sum expression
			static
			CExpression *PexprLogicalGbAggWithSum(IMemoryPool *pmp);

			// generate a random group by over join expression
			static
			CExpression *PexprLogicalGbAggOverJoin(IMemoryPool *pmp);

			// generate GbAgg with correlation beneath it
			static
			CExpression *PexprLogicalGbAggCorrelated(IMemoryPool *pmp);

			// generate a dedup group by over inner join expression
			static
			CExpression *PexprLogicalGbAggDedupOverInnerJoin(IMemoryPool *pmp);

			// generate a top-level apply expression
			template<class T>
			static
			CExpression *PexprLogicalApply(IMemoryPool *pmp);

			// generate a top-level apply expression with an outer reference
			template<class T>
			static
			CExpression *PexprLogicalApplyWithOuterRef(IMemoryPool *pmp);

			// generate a const table get expression
			static
			CExpression *PexprConstTableGet(IMemoryPool *pmp, ULONG ulElements);
			
			// generate a const table get expression with 5 elements
			static
			CExpression *PexprConstTableGet5(IMemoryPool *pmp);

			// generate a logical insert expression
			static
			CExpression *PexprLogicalInsert(IMemoryPool *pmp);

			// generate a logical delete expression
			static
			CExpression *PexprLogicalDelete(IMemoryPool *pmp);

			// generate a logical update expression
			static
			CExpression *PexprLogicalUpdate(IMemoryPool *pmp);

			// generate a dynamic get expression
			static
			CExpression *PexprLogicalDynamicGet
							(
							IMemoryPool *pmp,
							CTableDescriptor *ptabdesc,
							const CWStringConst *pstrTableAlias,
							ULONG ulPartIndex
							);
			
			// generate a dynamic get expression
			static
			CExpression *PexprLogicalDynamicGet(IMemoryPool *pmp);

			// generate a select with an equality predicate over a dynamic get expression
			static
			CExpression *PexprLogicalSelectWithEqPredicateOverDynamicGet(IMemoryPool *pmp);
			
			// generate a select with a less than predicate over a dynamic get expression
			static
			CExpression *PexprLogicalSelectWithLTPredicateOverDynamicGet(IMemoryPool *pmp);
			
			// generate a logical table-valued function expression with 2 constant arguments
			static
			CExpression *PexprLogicalTVFTwoArgs(IMemoryPool *pmp);

			// generate a logical table-valued function expression with 3 constant arguments
			static
			CExpression *PexprLogicalTVFThreeArgs(IMemoryPool *pmp);

			// generate a logical table-valued function expression with no arguments
			static
			CExpression *PexprLogicalTVFNoArgs(IMemoryPool *pmp);

			// generate a logical table-valued function expression
			static
			CExpression *PexprLogicalTVF(IMemoryPool *pmp, ULONG ulArgs);

			// generate a cte producer on top of a select
			static
			CExpression *PexprLogicalCTEProducerOverSelect(IMemoryPool *pmp, ULONG ulCTEId);

			// generate a CTE producer on top of a select
			static
			CExpression *PexprLogicalCTEProducerOverSelect(IMemoryPool *pmp);

			// generate an expression with CTE producer and consumer
			static
			CExpression *PexprCTETree(IMemoryPool *pmp);

			// generate a logical sequence expression
			static
			CExpression *PexprLogicalSequence(IMemoryPool *pmp, DrgPexpr *pdrgpexpr); 

			// generate a logical sequence expression
			static
			CExpression *PexprLogicalSequence(IMemoryPool *pmp);

			// generate a random union expression
			static
			CExpression *PexprLogicalUnion(IMemoryPool *pmp, ULONG ulDepth);

			// generate a sequence project expression
			static
			CExpression *PexprLogicalSequenceProject(IMemoryPool *pmp, OID oidFunc, CExpression *pexprInput);

			// generate a random expression with one window function
			static
			CExpression *PexprOneWindowFunction(IMemoryPool *pmp);

			// generate a random expression with two window functions
			static
			CExpression *PexprTwoWindowFunctions(IMemoryPool *pmp);

			// generate a dynamic array of join expressions
			static
			DrgPexprJoins *PdrgpexprJoins(IMemoryPool *pmp, CWStringConst *pstrRel, ULONG *pulRel, ULONG ulRels, BOOL fCrossProduct);
							
			// generate a query context from an array of required column references
			static
			CQueryContext *PqcGenerate(IMemoryPool *pmp, CExpression *pexpr, DrgPcr *pdrgpcr);

			// generate a dummy query context for testing
			static
			CQueryContext *PqcGenerate(IMemoryPool *pmp, CExpression *pexpr);

			// generate a nested AND/OR tree
			static
			CExpression *PexprScalarNestedPreds(IMemoryPool *pmp, CExpression *pexpr, CScalarBoolOp::EBoolOperator eboolop);

			// equality predicate shortcut
			static
			void EqualityPredicate(IMemoryPool *pmp, CColRefSet *pcrsLeft, CColRefSet *pcrsRight, DrgPexpr *pdrgpexpr);

			// int2 point
			static
			CPoint *PpointInt2(IMemoryPool *pmp, INT i);

			// int4 point
			static
			CPoint *PpointInt4(IMemoryPool *pmp, INT i);

			// create an INT4 point with null value
			static
			CPoint *PpointInt4NullVal(IMemoryPool *pmp);

			// int8 point
			static
			CPoint *PpointInt8(IMemoryPool *pmp, INT li);

			// bool point
			static
			CPoint *PpointBool(IMemoryPool *pmp, BOOL fVal);

			// perform DXL(query) -> CExpression(Log) ->CExpression(Ph) -> DXL(plan)
			// and check whether the resulting plan DXL matches the expected output
			static
			GPOS_RESULT EresTranslate
								(
								IMemoryPool *pmp,
								const CHAR *szQueryFileName,
								const CHAR *szPlanFileName,
								BOOL fIgnoreMismatch
								);

			// return query logical expression from minidump
			static
			CExpression *PexprReadQuery
				(
				IMemoryPool *pmp,
				const CHAR *szQueryFileName
				);

			// compare expected and actual output
			static
			GPOS_RESULT EresCompare
								(
								IOstream &os,
								CWStringDynamic *pstrActual,
								CWStringDynamic *pstrExpected,
								BOOL fIgnoreMismatch = true
								);
			
			// match given two plans using string comparison
			static
			BOOL FPlanMatch
				(
				IMemoryPool *pmp,
				IOstream &os,
				const CDXLNode *pdxlnFst,
				ULLONG ullPlanIdFst,
				ULLONG ullPlanSpaceSizeFst,
				const CDXLNode *pdxlnSnd,
				ULLONG ullPlanIdSnd,
				ULLONG ullPlanSpaceSizeSnd
				);

			// compare given DXL plans
			static
			BOOL FPlanCompare
				(
				IMemoryPool *pmp,
				IOstream &os,
				const CDXLNode *pdxlnFst,
				ULLONG ullPlanIdFst,
				ULLONG ullPlanSpaceSizeFst,
				const CDXLNode *pdxlnSnd,
				ULLONG ullPlanIdSnd,
				ULLONG ullPlanSpaceSizeSnd,
				BOOL fMatchPlans,
				INT iCmpSpaceSize
				);

			// run minidumps while loading metadata from passed file
			static
			GPOS_RESULT EresRunMinidumps
				(
				IMemoryPool *pmp,
				const CHAR *rgszFileNames[],
				ULONG ulTests,
				ULONG *pulTestCounter,
				ULONG ulSessionId,
				ULONG ulCmdId,
				BOOL fMatchPlans,
				BOOL fTestSpacePruning,
				const CHAR *szMDFilePath =  NULL,
				IConstExprEvaluator *pceeval = NULL
				);

			// run all minidumps based on one metadata file
			static
			GPOS_RESULT EresRunMinidumpsUsingOneMDFile
				(
				IMemoryPool *pmp,
				const CHAR *szMDFilePath,
				const CHAR *rgszFileNames[],
				ULONG ulTests,
				ULONG *pulTestCounter,
				ULONG ulSessionId,
				ULONG ulCmdId,
				BOOL fMatchPlans,
				INT iCmpSpaceSize,
				IConstExprEvaluator *pceeval = NULL
				);

			// run one minidump-based test using passed MD Accessor
			static
			GPOS_RESULT EresRunMinidump
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				const CHAR *rgszFileName,
				ULONG *pulTestCounter,
				ULONG ulSessionId,
				ULONG ulCmdId,
				BOOL fMatchPlans,
				INT iCmpSpaceSize,
				IConstExprEvaluator *pceeval
				);

			// test plan sampling
			static
			GPOS_RESULT EresSamplePlans
				(
				const CHAR *rgszFileNames[],
				ULONG ulTests,
				ULONG *pulTestCounter,
				ULONG ulSessionId,
				ULONG ulCmdId
				);
			
			// check all enumerated plans using given PlanChecker function
			static
			GPOS_RESULT EresCheckPlans
				(
				const CHAR *rgszFileNames[],
				ULONG ulTests,
				ULONG *pulTestCounter,
				ULONG ulSessionId,
				ULONG ulCmdId,
				FnPlanChecker *pfpc
				);

			// check the optimized plan using given DXLPlanChecker function
			// cost model must be calibrated
			// does not take ownership of pdrgpcp
			static
			GPOS_RESULT EresCheckOptimizedPlan
				(
				const CHAR *rgszFileNames[],
				ULONG ulTests,
				ULONG *pulTestCounter,
				ULONG ulSessionId,
				ULONG ulCmdId,
				FnDXLPlanChecker *pfdpc,
				DrgPcp *pdrgpcp
				);

			// generate cost model used in tests
			static
			ICostModel *Pcm
				(
				IMemoryPool *pmp
				)
			{
				return GPOS_NEW(pmp) CCostModelGPDBLegacy(pmp, GPOPT_TEST_SEGMENTS);
			}

			// create a datum with a given type, encoded value and int value
			static
			IDatum *PdatumGeneric(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdidType, CWStringDynamic *pstrEncodedValue, LINT lValue);

			// create an interval for generic data types
			// does not take ownership of pmdidType
			static
			CConstraintInterval *PciGenericInterval
			        (
			        IMemoryPool *pmp,
			        CMDAccessor *pmda,
			        const CMDIdGPDB &mdidType,
			        CColRef *pcr,
			        CWStringDynamic *pstrLower,
			        LINT lLower,
			        CRange::ERangeInclusion eriLeft,
			        CWStringDynamic *pstrUpper,
			        LINT lUpper,
			        CRange::ERangeInclusion eriRight
			        );

			// helper for generating a scalar compare ident to constant
			static
			CExpression *PexprScalarCmpIdentToConstant(IMemoryPool *pmp, CExpression *pexpr);

			// helper for generating an exists subquery
			static
			CExpression *PexprExistsSubquery(IMemoryPool *pmp, CExpression *pexprOuter);

			// helper for generating a not exists subquery
			static
			CExpression *PexprNotExistsSubquery(IMemoryPool *pmp, CExpression *pexprOuter);

			// helper for generating an ANY subquery
			static
			CExpression *PexpSubqueryAny(IMemoryPool *pmp, CExpression *pexprOuter);

			// helper for generating an ALL subquery
			static
			CExpression *PexpSubqueryAll(IMemoryPool *pmp, CExpression *pexprOuter);

			// recursively traverses the subtree rooted at the given expression, and return
			// the first subexpression it encounters that has the given id
			static
			const CExpression *PexprFirst(const CExpression *pexpr, const COperator::EOperatorId eopid);

			// generate a scalar expression comparing scalar identifier to a constant
			static
			CExpression *PexprScIdentCmpConst(IMemoryPool *pmp, CExpression *pexprRelational, IMDType::ECmpType ecmpt, ULONG ulVal);

			// generate a scalar expression comparing scalar identifiers
			static
			CExpression *PexprScIdentCmpScIdent(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight, IMDType::ECmpType ecmpt);

			// generate a scalar AND expression
			static
			CExpression *PexprAnd(IMemoryPool *pmp, CExpression *pexprFst, CExpression *pexprSnd);

			// generate a scalar OR expression
			static
			CExpression *PexprOr(IMemoryPool *pmp, CExpression *pexprFst, CExpression *pexprSnd);

#ifdef GPOS_DEBUG
			static
			BOOL FFaultSimulation();
#endif // GPOS_DEBUG
			
			// default file-based metadata provider
			static
			CMDProviderMemory *m_pmdpf;

			// default source system id
			static
			CSystemId m_sysidDefault;

			// metadata file
			static
			const CHAR *m_szMDFileName;
			
			// xsd file
			static
			const CHAR *m_szXSDPath;

			// run Minidump-based tests in the given array of files
			static
			GPOS_RESULT EresUnittest_RunTests(const CHAR **rgszFileNames, ULONG *pulTestCounter, ULONG ulTests);

	}; // class CTestUtils

} // namespace gpopt

// include implementation of templated functions
#include "CTestUtils.inl"

#endif // !GPOPT_CTestUtils_H

// EOF
