//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorExprToDXL.cpp
//
//	@doc:
//		Implementation of the methods for translating Optimizer physical expression
//		trees into DXL.
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CHashMap.h"

#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/CMDRelationCtasGPDB.h"

#include "naucrates/dxl/operators/CDXLDatumBool.h"
#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"
#include "naucrates/dxl/operators/CDXLWindowFrame.h"
#include "naucrates/dxl/operators/dxlops.h"

#include "naucrates/statistics/CStatistics.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalMotionRandom.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXLUtils.h"

#include "naucrates/base/IDatumInt8.h"
#include "naucrates/base/CDatumBoolGPDB.h"

#include "naucrates/traceflags/traceflags.h"

using namespace gpos;
using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;
using namespace gpnaucrates;

#define GPOPT_MASTER_SEGMENT_ID (-1)

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::CTranslatorExprToDXL
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CTranslatorExprToDXL::CTranslatorExprToDXL
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	DrgPi *pdrgpiSegments,
	BOOL fInitColumnFactory
	)
	:
	m_pmp(pmp),
	m_pmda(pmda),
	m_pdpplan(NULL),
	m_pcf(NULL),
	m_pdrgpiSegments(pdrgpiSegments),
	m_iMasterId(GPOPT_MASTER_SEGMENT_ID)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT_IMP(NULL != pdrgpiSegments, (0 < pdrgpiSegments->UlSafeLength()));

	InitScalarTranslators();
	InitPhysicalTranslators();

	// initialize hash map
	m_phmcrdxln = GPOS_NEW(m_pmp) HMCrDxln(m_pmp);

	m_phmcrdxlnIndexLookup = GPOS_NEW(m_pmp) HMCrDxln(m_pmp);

	if (fInitColumnFactory)
	{
		// get column factory from optimizer context object
		m_pcf = COptCtxt::PoctxtFromTLS()->Pcf();
		GPOS_ASSERT(NULL != m_pcf);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::~CTranslatorExprToDXL
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CTranslatorExprToDXL::~CTranslatorExprToDXL()
{
	CRefCount::SafeRelease(m_pdrgpiSegments);
	m_phmcrdxln->Release();
	m_phmcrdxlnIndexLookup->Release();
	CRefCount::SafeRelease(m_pdpplan);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::InitScalarTranslators
//
//	@doc:
//		Initialize index of scalar translators
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::InitScalarTranslators()
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(m_rgpfScalarTranslators); ul++)
	{
		m_rgpfScalarTranslators[ul] = NULL;
	}

	// array mapping operator type to translator function
	SScTranslatorMapping rgScalarTranslators[] = 
	{
			{COperator::EopScalarIdent, &gpopt::CTranslatorExprToDXL::PdxlnScId},
			{COperator::EopScalarCmp, &gpopt::CTranslatorExprToDXL::PdxlnScCmp},
			{COperator::EopScalarIsDistinctFrom, &gpopt::CTranslatorExprToDXL::PdxlnScDistinctCmp},
			{COperator::EopScalarOp, &gpopt::CTranslatorExprToDXL::PdxlnScOp},
			{COperator::EopScalarBoolOp, &gpopt::CTranslatorExprToDXL::PdxlnScBoolExpr},
			{COperator::EopScalarConst, &gpopt::CTranslatorExprToDXL::PdxlnScConst},
			{COperator::EopScalarFunc, &gpopt::CTranslatorExprToDXL::PdxlnScFuncExpr},
			{COperator::EopScalarWindowFunc, &gpopt::CTranslatorExprToDXL::PdxlnScWindowFuncExpr},
			{COperator::EopScalarAggFunc,&gpopt::CTranslatorExprToDXL::PdxlnScAggref},
			{COperator::EopScalarNullIf, &gpopt::CTranslatorExprToDXL::PdxlnScNullIf},
			{COperator::EopScalarNullTest, &gpopt::CTranslatorExprToDXL::PdxlnScNullTest},
			{COperator::EopScalarBooleanTest, &gpopt::CTranslatorExprToDXL::PdxlnScBooleanTest},
			{COperator::EopScalarIf, &gpopt::CTranslatorExprToDXL::PdxlnScIfStmt},
			{COperator::EopScalarSwitch, &gpopt::CTranslatorExprToDXL::PdxlnScSwitch},
			{COperator::EopScalarSwitchCase, &gpopt::CTranslatorExprToDXL::PdxlnScSwitchCase},
			{COperator::EopScalarCaseTest, &gpopt::CTranslatorExprToDXL::PdxlnScCaseTest},
			{COperator::EopScalarCoalesce, &gpopt::CTranslatorExprToDXL::PdxlnScCoalesce},
			{COperator::EopScalarMinMax, &gpopt::CTranslatorExprToDXL::PdxlnScMinMax},
			{COperator::EopScalarCast, &gpopt::CTranslatorExprToDXL::PdxlnScCast},
			{COperator::EopScalarCoerceToDomain, &gpopt::CTranslatorExprToDXL::PdxlnScCoerceToDomain},
			{COperator::EopScalarCoerceViaIO, &gpopt::CTranslatorExprToDXL::PdxlnScCoerceViaIO},
			{COperator::EopScalarArray, &gpopt::CTranslatorExprToDXL::PdxlnArray},
			{COperator::EopScalarArrayCmp, &gpopt::CTranslatorExprToDXL::PdxlnArrayCmp},
			{COperator::EopScalarArrayRef, &gpopt::CTranslatorExprToDXL::PdxlnArrayRef},
			{COperator::EopScalarArrayRefIndexList, &gpopt::CTranslatorExprToDXL::PdxlnArrayRefIndexList},
			{COperator::EopScalarAssertConstraintList, &gpopt::CTranslatorExprToDXL::PdxlnAssertPredicate},
			{COperator::EopScalarAssertConstraint, &gpopt::CTranslatorExprToDXL::PdxlnAssertConstraint},
			{COperator::EopScalarDMLAction, &gpopt::CTranslatorExprToDXL::PdxlnDMLAction},
			{COperator::EopScalarBitmapIndexProbe, &gpopt::CTranslatorExprToDXL::PdxlnBitmapIndexProbe},
			{COperator::EopScalarBitmapBoolOp, &gpopt::CTranslatorExprToDXL::PdxlnBitmapBoolOp},
	};

	const ULONG ulTranslators = GPOS_ARRAY_SIZE(rgScalarTranslators);
	for (ULONG ul = 0; ul < ulTranslators; ul++)
	{
		SScTranslatorMapping elem = rgScalarTranslators[ul];
		m_rgpfScalarTranslators[elem.eopid] = elem.pf;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::InitPhysicalTranslators
//
//	@doc:
//		Initialize index of physical translators
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::InitPhysicalTranslators()
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(m_rgpfPhysicalTranslators); ul++)
	{
		m_rgpfPhysicalTranslators[ul] = NULL;
	}

	// array mapping operator type to translator function
	SPhTranslatorMapping rgPhysicalTranslators[] = 
	{
			{COperator::EopPhysicalFilter, &gpopt::CTranslatorExprToDXL::PdxlnResult},
			{COperator::EopPhysicalIndexScan, &gpopt::CTranslatorExprToDXL::PdxlnIndexScan},
			{COperator::EopPhysicalBitmapTableScan, &gpopt::CTranslatorExprToDXL::PdxlnBitmapTableScan},
			{COperator::EopPhysicalComputeScalar, &gpopt::CTranslatorExprToDXL::PdxlnComputeScalar},
			{COperator::EopPhysicalScalarAgg, &gpopt::CTranslatorExprToDXL::PdxlnAggregate},
			{COperator::EopPhysicalHashAgg, &gpopt::CTranslatorExprToDXL::PdxlnAggregate},
			{COperator::EopPhysicalStreamAgg, &gpopt::CTranslatorExprToDXL::PdxlnAggregate},
			{COperator::EopPhysicalHashAggDeduplicate, &gpopt::CTranslatorExprToDXL::PdxlnAggregateDedup},
			{COperator::EopPhysicalStreamAggDeduplicate, &gpopt::CTranslatorExprToDXL::PdxlnAggregateDedup},
			{COperator::EopPhysicalSort, &gpopt::CTranslatorExprToDXL::PdxlnSort},
			{COperator::EopPhysicalLimit, &gpopt::CTranslatorExprToDXL::PdxlnLimit},
			{COperator::EopPhysicalSequenceProject, &gpopt::CTranslatorExprToDXL::PdxlnWindow},
			{COperator::EopPhysicalInnerNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnNLJoin},
			{COperator::EopPhysicalInnerIndexNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnNLJoin},
			{COperator::EopPhysicalCorrelatedInnerNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnCorrelatedNLJoin},
			{COperator::EopPhysicalLeftOuterNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnNLJoin},
			{COperator::EopPhysicalCorrelatedLeftOuterNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnCorrelatedNLJoin},
			{COperator::EopPhysicalCorrelatedLeftSemiNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnCorrelatedNLJoin},
			{COperator::EopPhysicalCorrelatedInLeftSemiNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnCorrelatedNLJoin},
			{COperator::EopPhysicalLeftSemiNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnNLJoin},
			{COperator::EopPhysicalLeftAntiSemiNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnNLJoin},
			{COperator::EopPhysicalLeftAntiSemiNLJoinNotIn, &gpopt::CTranslatorExprToDXL::PdxlnNLJoin},
			{COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnCorrelatedNLJoin},
			{COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin, &gpopt::CTranslatorExprToDXL::PdxlnCorrelatedNLJoin},
			{COperator::EopPhysicalInnerHashJoin, &gpopt::CTranslatorExprToDXL::PdxlnHashJoin},
			{COperator::EopPhysicalLeftOuterHashJoin, &gpopt::CTranslatorExprToDXL::PdxlnHashJoin},
			{COperator::EopPhysicalLeftSemiHashJoin, &gpopt::CTranslatorExprToDXL::PdxlnHashJoin},
			{COperator::EopPhysicalLeftAntiSemiHashJoin, &gpopt::CTranslatorExprToDXL::PdxlnHashJoin},
			{COperator::EopPhysicalLeftAntiSemiHashJoinNotIn, &gpopt::CTranslatorExprToDXL::PdxlnHashJoin},
			{COperator::EopPhysicalMotionGather,&gpopt::CTranslatorExprToDXL::PdxlnMotion},
			{COperator::EopPhysicalMotionBroadcast,&gpopt::CTranslatorExprToDXL::PdxlnMotion},
			{COperator::EopPhysicalMotionHashDistribute,&gpopt::CTranslatorExprToDXL::PdxlnMotion},
			{COperator::EopPhysicalMotionRoutedDistribute,&gpopt::CTranslatorExprToDXL::PdxlnMotion},
			{COperator::EopPhysicalMotionRandom, &gpopt::CTranslatorExprToDXL::PdxlnMotion},
			{COperator::EopPhysicalSpool, &gpopt::CTranslatorExprToDXL::PdxlnMaterialize},
			{COperator::EopPhysicalSequence, &gpopt::CTranslatorExprToDXL::PdxlnSequence},
			{COperator::EopPhysicalDynamicTableScan, &gpopt::CTranslatorExprToDXL::PdxlnDynamicTableScan},
			{COperator::EopPhysicalDynamicBitmapTableScan, &gpopt::CTranslatorExprToDXL::PdxlnDynamicBitmapTableScan},
			{COperator::EopPhysicalDynamicIndexScan, &gpopt::CTranslatorExprToDXL::PdxlnDynamicIndexScan},
			{COperator::EopPhysicalPartitionSelector, &gpopt::CTranslatorExprToDXL::PdxlnPartitionSelector},
			{COperator::EopPhysicalPartitionSelectorDML, &gpopt::CTranslatorExprToDXL::PdxlnPartitionSelectorDML},
			{COperator::EopPhysicalConstTableGet, &gpopt::CTranslatorExprToDXL::PdxlnResultFromConstTableGet},
			{COperator::EopPhysicalTVF, &gpopt::CTranslatorExprToDXL::PdxlnTVF},
			{COperator::EopPhysicalUnionAll, &gpopt::CTranslatorExprToDXL::PdxlnAppend},
			{COperator::EopPhysicalDML, &gpopt::CTranslatorExprToDXL::PdxlnDML},
			{COperator::EopPhysicalSplit, &gpopt::CTranslatorExprToDXL::PdxlnSplit},
			{COperator::EopPhysicalRowTrigger, &gpopt::CTranslatorExprToDXL::PdxlnRowTrigger},
			{COperator::EopPhysicalAssert, &gpopt::CTranslatorExprToDXL::PdxlnAssert},
			{COperator::EopPhysicalCTEProducer, &gpopt::CTranslatorExprToDXL::PdxlnCTEProducer},
			{COperator::EopPhysicalCTEConsumer, &gpopt::CTranslatorExprToDXL::PdxlnCTEConsumer},
	};

	const ULONG ulTranslators = GPOS_ARRAY_SIZE(rgPhysicalTranslators);
	for (ULONG ul = 0; ul < ulTranslators; ul++)
	{
		SPhTranslatorMapping elem = rgPhysicalTranslators[ul];
		m_rgpfPhysicalTranslators[elem.eopid] = elem.pf;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnTranslate
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnTranslate
	(
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPmdname *pdrgpmdname
	)
{
	CAutoTimer at("\n[OPT]: Expr To DXL Translation Time", GPOS_FTRACE(EopttracePrintOptStats));

	GPOS_ASSERT(NULL == m_pdpplan);
	
	m_pdpplan = CDrvdPropPlan::Pdpplan(pexpr->PdpDerive());
	m_pdpplan->AddRef();

	DrgPds *pdrgpdsBaseTables = GPOS_NEW(m_pmp) DrgPds(m_pmp);
	ULONG ulNonGatherMotions = 0;
	BOOL fDML = false;
	CDXLNode *pdxln = Pdxln(pexpr, pdrgpcr, pdrgpdsBaseTables, &ulNonGatherMotions, &fDML, true /*fRemap*/, true /*fRoot*/);

	if (fDML)
	{
		pdrgpdsBaseTables->Release();
		return pdxln;
	}

	CDXLNode *pdxlnPrL = (*pdxln)[0];
	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());

	const ULONG ulLen = pdrgpmdname->UlLength();
	GPOS_ASSERT(ulLen == pdrgpcr->UlLength());
	GPOS_ASSERT(ulLen == pdxlnPrL->UlArity());
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		// desired output column name
		CMDName *pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, (*pdrgpmdname)[ul]->Pstr());

		// get the old project element for the ColId
		CDXLNode *pdxlnPrElOld = (*pdxlnPrL)[ul];
		CDXLScalarProjElem *pdxlopPrElOld = CDXLScalarProjElem::PdxlopConvert(pdxlnPrElOld->Pdxlop());
		GPOS_ASSERT(1 == pdxlnPrElOld->UlArity());
		CDXLNode *pdxlnChild = (*pdxlnPrElOld)[0];
		const ULONG ulColId = pdxlopPrElOld->UlId();

		// create a new project element node with the col id and new column name
		// and add the scalar child
		CDXLNode *pdxlnPrElNew = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarProjElem(m_pmp, ulColId, pmdname));
		pdxlnChild->AddRef();
		pdxlnPrElNew->AddChild(pdxlnChild);

		// replace the project element
		pdxlnPrL->ReplaceChild(ul, pdxlnPrElNew);
	}

	

	if (0 == ulNonGatherMotions)
	{
		CDrvdPropRelational *pdprel =  CDrvdPropRelational::Pdprel(pexpr->Pdp(CDrvdProp::EptRelational));
		CTranslatorExprToDXLUtils::SetDirectDispatchInfo(m_pmp, m_pmda, pdxln, pdprel, pdrgpdsBaseTables);
	}
	
	pdrgpdsBaseTables->Release();
	return pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Pdxln
//
//	@doc:
//		Translates an optimizer physical expression tree into DXL.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::Pdxln
	(
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	BOOL fRemap,
	BOOL fRoot
	)
{
	GPOS_ASSERT(NULL != pexpr);
	ULONG ulOpId =  (ULONG) pexpr->Pop()->Eopid();
	if (COperator::EopPhysicalTableScan == ulOpId || COperator::EopPhysicalExternalScan == ulOpId)
	{
		CDXLNode *pdxln = PdxlnTblScan(pexpr, NULL /*pcrsOutput*/, pdrgpcr, pdrgpdsBaseTables, NULL /* pexprScalarCond */, NULL /* cost info */);
		CTranslatorExprToDXLUtils::SetStats(m_pmp, m_pmda, pdxln, pexpr->Pstats(), fRoot);
		
		return pdxln;
	}
	PfPdxlnPhysical pf = m_rgpfPhysicalTranslators[ulOpId];
	if (NULL == pf)
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, pexpr->Pop()->SzId());
	}

	// add a result node on top to project out columns not needed any further,
	// for instance, if the grouping /order by /partition/ distribution columns
	// are no longer needed
	CDXLNode *pdxlnNew = NULL;

	CDXLNode *pdxln = (this->* pf)(pexpr, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);

	if (!fRemap || EdxlopPhysicalDML == pdxln->Pdxlop()->Edxlop())
	{
		pdxlnNew = pdxln;
	}
	else
	{
		DrgPcr *pdrgpcrRequired = NULL;
		
		if (EdxlopPhysicalCTAS == pdxln->Pdxlop()->Edxlop())
		{
			pdrgpcr->AddRef();
			pdrgpcrRequired = pdrgpcr;
		}
		else
		{
			pdrgpcrRequired = pexpr->Prpp()->PcrsRequired()->Pdrgpcr(m_pmp);
		}
		pdxlnNew = PdxlnRemapOutputColumns(pexpr, pdxln, pdrgpcrRequired, pdrgpcr);
		pdrgpcrRequired->Release();
	}

	if (NULL == pdxlnNew->Pdxlprop()->Pdxlstatsderrel())
	{
		CTranslatorExprToDXLUtils::SetStats(m_pmp, m_pmda, pdxlnNew, pexpr->Pstats(), fRoot);
	}
	
	return pdxlnNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScalar
//
//	@doc:
//		Translates an optimizer scalar expression tree into DXL. Any column
//		refs that are members of the input colrefset are replaced by the input
//		subplan node
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScalar
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	ULONG ulOpId =  (ULONG) pexpr->Pop()->Eopid();
	PfPdxlnScalar pf = m_rgpfScalarTranslators[ulOpId];

	if (NULL == pf)
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, pexpr->Pop()->SzId());
	}
	
	return (this->* pf)(pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnTblScan
//
//	@doc:
//		Create a DXL table scan node from an optimizer table scan node
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnTblScan
	(
	CExpression *pexprTblScan,
	CColRefSet *pcrsOutput,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	CExpression *pexprScalar,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexprTblScan);

	CPhysicalTableScan *popTblScan = CPhysicalTableScan::PopConvert(pexprTblScan->Pop());
	DrgPcr *pdrgpcrOutput = popTblScan->PdrgpcrOutput();
	
	// translate table descriptor
	CDXLTableDescr *pdxltabdesc = Pdxltabdesc(popTblScan->Ptabdesc(), pdrgpcrOutput);

	// construct plan costs, if there are not passed as a parameter
	if (NULL == pdxlprop)
	{
		pdxlprop = Pdxlprop(pexprTblScan);
	}

	// construct scan operator
	CDXLPhysicalTableScan *pdxlopTS = NULL;
	COperator::EOperatorId eopid = pexprTblScan->Pop()->Eopid();
	if (COperator::EopPhysicalTableScan == eopid)
	{
		pdxlopTS = GPOS_NEW(m_pmp) CDXLPhysicalTableScan(m_pmp, pdxltabdesc);
	}
	else
	{
		GPOS_ASSERT(COperator::EopPhysicalExternalScan == eopid);
		pdxlopTS = GPOS_NEW(m_pmp) CDXLPhysicalExternalScan(m_pmp, pdxltabdesc);
	}
	
	CDXLNode *pdxlnTblScan = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopTS);
	pdxlnTblScan->SetProperties(pdxlprop);
	
	// construct projection list
	GPOS_ASSERT(NULL != pexprTblScan->Prpp());

	// if the output columns are passed from above then use them
	if (NULL == pcrsOutput)
	{
	  pcrsOutput = pexprTblScan->Prpp()->PcrsRequired();
	}
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcr);

	CDXLNode *pdxlnCond = NULL;
	if (NULL != pexprScalar)
	{
	  pdxlnCond = PdxlnScalar(pexprScalar);
	}

	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnCond);
	
	// add children in the right order
	pdxlnTblScan->AddChild(pdxlnPrL); 		// project list
	pdxlnTblScan->AddChild(pdxlnFilter);	// filter
	
#ifdef GPOS_DEBUG
	pdxlnTblScan->Pdxlop()->AssertValid(pdxlnTblScan, false /* fValidateChildren */);
#endif
	
	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(pexprTblScan->Pdp(CDrvdProp::EptPlan))->Pds();
	pds->AddRef();
	pdrgpdsBaseTables->Append(pds);
	return pdxlnTblScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnIndexScan
//
//	@doc:
//		Create a DXL index scan node from an optimizer index scan node based
//		on passed properties
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnIndexScan
	(
	CExpression *pexprIndexScan,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	BOOL * // pfDML
	)
{
	GPOS_ASSERT(NULL != pexprIndexScan);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprIndexScan);

	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(pexprIndexScan->Pdp(CDrvdProp::EptPlan))->Pds();
	pds->AddRef();
	pdrgpdsBaseTables->Append(pds);
	return PdxlnIndexScan(pexprIndexScan, pdrgpcr, pdxlprop, pexprIndexScan->Prpp());
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnIndexScan
//
//	@doc:
//		Create a DXL index scan node from an optimizer index scan node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnIndexScan
	(
	CExpression *pexprIndexScan,
	DrgPcr *pdrgpcr,
	CDXLPhysicalProperties *pdxlprop,
	CReqdPropPlan *prpp
	)
{
	GPOS_ASSERT(NULL != pexprIndexScan);
	GPOS_ASSERT(NULL != pdxlprop);
	GPOS_ASSERT(NULL != prpp);

	CPhysicalIndexScan *popIs = CPhysicalIndexScan::PopConvert(pexprIndexScan->Pop());

	DrgPcr *pdrgpcrOutput = popIs->PdrgpcrOutput();

	// translate table descriptor
	CDXLTableDescr *pdxltabdesc = Pdxltabdesc(popIs->Ptabdesc(), pdrgpcrOutput);

	// create index descriptor
	CIndexDescriptor *pindexdesc = popIs->Pindexdesc();
	CMDName *pmdnameIndex = GPOS_NEW(m_pmp) CMDName(m_pmp, pindexdesc->Name().Pstr());
	IMDId *pmdidIndex = pindexdesc->Pmdid();
	pmdidIndex->AddRef();
	CDXLIndexDescr *pdxlid = GPOS_NEW(m_pmp) CDXLIndexDescr(m_pmp, pmdidIndex, pmdnameIndex);

	// TODO: vrgahavan; we assume that the index are always forward access.
	// create the physical index scan operator
	CDXLPhysicalIndexScan *pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalIndexScan(m_pmp, pdxltabdesc, pdxlid, EdxlisdForward);
	CDXLNode *pdxlnIndexScan = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// set properties
	pdxlnIndexScan->SetProperties(pdxlprop);

	// translate project list
	CColRefSet *pcrsOutput = prpp->PcrsRequired();
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcr);

	// translate index predicates
	CExpression *pexprCond = (*pexprIndexScan)[0];
	CDXLNode *pdxlnIndexCondList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarIndexCondList(m_pmp));

	DrgPexpr *pdrgpexprConds = CPredicateUtils::PdrgpexprConjuncts(m_pmp, pexprCond);
	const ULONG ulLength = pdrgpexprConds->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		CExpression *pexprIndexCond = (*pdrgpexprConds)[ul];
		CDXLNode *pdxlnIndexCond = PdxlnScalar(pexprIndexCond);
		pdxlnIndexCondList->AddChild(pdxlnIndexCond);
	}
	pdrgpexprConds->Release();

	CDXLNode *pdxlnResidualCond = NULL;
	if (2 == pexprIndexScan->UlArity())
	{
		// translate residual predicates into the filter node
		CExpression *pexprResidualCond = (*pexprIndexScan)[1];
		if (COperator::EopScalarConst != pexprResidualCond->Pop()->Eopid())
		{
			pdxlnResidualCond = PdxlnScalar(pexprResidualCond);
		}
	}

	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnResidualCond);

	pdxlnIndexScan->AddChild(pdxlnPrL);
	pdxlnIndexScan->AddChild(pdxlnFilter);
	pdxlnIndexScan->AddChild(pdxlnIndexCondList);

#ifdef GPOS_DEBUG
	pdxlnIndexScan->Pdxlop()->AssertValid(pdxlnIndexScan, false /* fValidateChildren */);
#endif


	return pdxlnIndexScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnBitmapIndexProbe
//
//	@doc:
//		Create a DXL scalar bitmap index probe from an optimizer
//		scalar bitmap index probe operator.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnBitmapIndexProbe
	(
	CExpression *pexprBitmapIndexProbe
	)
{
	GPOS_ASSERT(NULL != pexprBitmapIndexProbe);
	CScalarBitmapIndexProbe *pop = CScalarBitmapIndexProbe::PopConvert(pexprBitmapIndexProbe->Pop());

	// create index descriptor
	CIndexDescriptor *pindexdesc = pop->Pindexdesc();
	CMDName *pmdnameIndex = GPOS_NEW(m_pmp) CMDName(m_pmp, pindexdesc->Name().Pstr());
	IMDId *pmdidIndex = pindexdesc->Pmdid();
	pmdidIndex->AddRef();

	CDXLIndexDescr *pdxlid = GPOS_NEW(m_pmp) CDXLIndexDescr(m_pmp, pmdidIndex, pmdnameIndex);
	CDXLScalarBitmapIndexProbe *pdxlop = GPOS_NEW(m_pmp) CDXLScalarBitmapIndexProbe(m_pmp, pdxlid);
	CDXLNode *pdxlnBitmapIndexProbe = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// translate index predicates
	CExpression *pexprCond = (*pexprBitmapIndexProbe)[0];
	CDXLNode *pdxlnIndexCondList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarIndexCondList(m_pmp));
	DrgPexpr *pdrgpexprConds = CPredicateUtils::PdrgpexprConjuncts(m_pmp, pexprCond);
	const ULONG ulLength = pdrgpexprConds->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		CExpression *pexprIndexCond = (*pdrgpexprConds)[ul];
		CDXLNode *pdxlnIndexCond = PdxlnScalar(pexprIndexCond);
		pdxlnIndexCondList->AddChild(pdxlnIndexCond);
	}
	pdrgpexprConds->Release();
	pdxlnBitmapIndexProbe->AddChild(pdxlnIndexCondList);

#ifdef GPOS_DEBUG
	pdxlnBitmapIndexProbe->Pdxlop()->AssertValid(pdxlnBitmapIndexProbe, false /*fValidateChildren*/);
#endif

	return pdxlnBitmapIndexProbe;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnBitmapBoolOp
//
//	@doc:
//		Create a DXL scalar bitmap boolean operator from an optimizer
//		scalar bitmap boolean operator operator.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnBitmapBoolOp
	(
	CExpression *pexprBitmapBoolOp
	)
{
	GPOS_ASSERT(NULL != pexprBitmapBoolOp);
	GPOS_ASSERT(2 == pexprBitmapBoolOp->UlArity());
	
	CScalarBitmapBoolOp *popBitmapBoolOp = CScalarBitmapBoolOp::PopConvert(pexprBitmapBoolOp->Pop());
	CExpression *pexprLeft = (*pexprBitmapBoolOp)[0];
	CExpression *pexprRight = (*pexprBitmapBoolOp)[1];
	
	CDXLNode *pdxlnLeft = PdxlnScalar(pexprLeft);
	CDXLNode *pdxlnRight = PdxlnScalar(pexprRight);
	
	IMDId *pmdidType = popBitmapBoolOp->PmdidType();
	pmdidType->AddRef();
	
	CDXLScalarBitmapBoolOp::EdxlBitmapBoolOp edxlbitmapop = CDXLScalarBitmapBoolOp::EdxlbitmapAnd;
	
	if (CScalarBitmapBoolOp::EbitmapboolOr == popBitmapBoolOp->Ebitmapboolop())
	{
		edxlbitmapop = CDXLScalarBitmapBoolOp::EdxlbitmapOr;
	}
	
	return GPOS_NEW(m_pmp) CDXLNode
						(
						m_pmp,
						GPOS_NEW(m_pmp) CDXLScalarBitmapBoolOp(m_pmp, pmdidType, edxlbitmapop),
						pdxlnLeft,
						pdxlnRight
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnBitmapTableScan
//
//	@doc:
//		Create a DXL physical bitmap table scan from an optimizer
//		physical bitmap table scan operator.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnBitmapTableScan
	(
	CExpression *pexprBitmapTableScan,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	BOOL * // pfDML
	)
{
	return PdxlnBitmapTableScan
			(
			pexprBitmapTableScan,
			NULL,  // pcrsOutput
			pdrgpcr,
			pdrgpdsBaseTables, 
			NULL, // pexprScalar
			NULL // pdxlprop
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::AddBitmapFilterColumns
//
//	@doc:
//		Add used columns in the bitmap recheck and the remaining scalar filter
//		condition to the required output column
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::AddBitmapFilterColumns
	(
	IMemoryPool *pmp,
	CPhysicalScan *pop,
	CExpression *pexprRecheckCond,
	CExpression *pexprScalar,
	CColRefSet *pcrsReqdOutput // append the required column reference
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(COperator::EopPhysicalDynamicBitmapTableScan == pop->Eopid() ||
				COperator::EopPhysicalBitmapTableScan == pop->Eopid());
	GPOS_ASSERT(NULL != pcrsReqdOutput);

	// compute what additional columns are required in the output of the (Dynamic) Bitmap Table Scan
	CColRefSet *pcrsAdditional =  GPOS_NEW(pmp) CColRefSet(pmp);

	if (NULL != pexprRecheckCond)
	{
		// add the columns used in the recheck condition
		pcrsAdditional->Include(CDrvdPropScalar::Pdpscalar(pexprRecheckCond->PdpDerive())->PcrsUsed());
	}

	if (NULL != pexprScalar)
	{
		// add the columns used in the filter condition
		pcrsAdditional->Include(CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed());
	}

	CColRefSet *pcrsBitmap =  GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsBitmap->Include(pop->PdrgpcrOutput());

	// only keep the columns that are in the table associated with the bitmap
	pcrsAdditional->Intersection(pcrsBitmap);

	if (0 < pcrsAdditional->CElements())
	{
		pcrsReqdOutput->Include(pcrsAdditional);
	}

	// clean up
	pcrsAdditional->Release();
	pcrsBitmap->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnBitmapTableScan
//
//	@doc:
//		Create a DXL physical bitmap table scan from an optimizer
//		physical bitmap table scan operator.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnBitmapTableScan
	(
	CExpression *pexprBitmapTableScan,
	CColRefSet *pcrsOutput,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	CExpression *pexprScalar,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexprBitmapTableScan);
	CPhysicalBitmapTableScan *pop = CPhysicalBitmapTableScan::PopConvert(pexprBitmapTableScan->Pop());

	// translate table descriptor
	CDXLTableDescr *pdxltabdesc = Pdxltabdesc(pop->Ptabdesc(), pop->PdrgpcrOutput());

	CDXLPhysicalBitmapTableScan *pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalBitmapTableScan(m_pmp, pdxltabdesc);
	CDXLNode *pdxlnBitmapTableScan = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// set properties
	// construct plan costs, if there are not passed as a parameter
	if (NULL == pdxlprop)
	{
		pdxlprop = Pdxlprop(pexprBitmapTableScan);
	}
	pdxlnBitmapTableScan->SetProperties(pdxlprop);

	// build projection list
	if (NULL == pcrsOutput)
	{
		pcrsOutput = pexprBitmapTableScan->Prpp()->PcrsRequired();
	}

	// translate scalar predicate into DXL filter only if it is not redundant
	CExpression *pexprRecheckCond = (*pexprBitmapTableScan)[0];
	CDXLNode *pdxlnCond = NULL;
	if (NULL != pexprScalar &&
		!CUtils::FScalarConstTrue(pexprScalar) &&
		!pexprScalar->FMatch(pexprRecheckCond))
	{
		pdxlnCond = PdxlnScalar(pexprScalar);
	}

	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnCond);

	CDXLNode *pdxlnRecheckCond = PdxlnScalar(pexprRecheckCond);
	CDXLNode *pdxlnRecheckCondFilter =
			GPOS_NEW(m_pmp) CDXLNode
						(
						m_pmp,
						GPOS_NEW(m_pmp) CDXLScalarRecheckCondFilter(m_pmp), pdxlnRecheckCond
						);

	AddBitmapFilterColumns(m_pmp, pop, pexprRecheckCond, pexprScalar, pcrsOutput);

	CDXLNode *pdxlnProjList = PdxlnProjList(pcrsOutput, pdrgpcr);

	// translate bitmap access path
	CDXLNode *pdxlnBitmapIndexPath = PdxlnScalar((*pexprBitmapTableScan)[1]);

	pdxlnBitmapTableScan->AddChild(pdxlnProjList);
	pdxlnBitmapTableScan->AddChild(pdxlnFilter);
	pdxlnBitmapTableScan->AddChild(pdxlnRecheckCondFilter);
	pdxlnBitmapTableScan->AddChild(pdxlnBitmapIndexPath);
#ifdef GPOS_DEBUG
	pdxlnBitmapTableScan->Pdxlop()->AssertValid(pdxlnBitmapTableScan, false /*fValidateChildren*/);
#endif
	
	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(pexprBitmapTableScan->Pdp(CDrvdProp::EptPlan))->Pds();
	pds->AddRef();
	pdrgpdsBaseTables->Append(pds);

	return pdxlnBitmapTableScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnDynamicTableScan
//
//	@doc:
//		Create a DXL dynamic table scan node from an optimizer 
//		dynamic table scan node.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnDynamicTableScan
	(
	CExpression *pexprDTS,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	BOOL * // pfDML
	)
{
	CExpression *pexprScalarCond = NULL;
	CDXLPhysicalProperties *pdxlprop = NULL;
	return PdxlnDynamicTableScan(pexprDTS, pdrgpcr, pdrgpdsBaseTables, pexprScalarCond, pdxlprop);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnDynamicTableScan
//
//	@doc:
//		Create a DXL dynamic table scan node from an optimizer 
//		dynamic table scan node.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnDynamicTableScan
	(
	CExpression *pexprDTS,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	CExpression *pexprScalarCond,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexprDTS);
	GPOS_ASSERT_IFF(NULL != pexprScalarCond, NULL != pdxlprop);
	
	CPhysicalDynamicTableScan *popDTS = CPhysicalDynamicTableScan::PopConvert(pexprDTS->Pop());	
	DrgPcr *pdrgpcrOutput = popDTS->PdrgpcrOutput();
	
	// translate table descriptor
	CDXLTableDescr *pdxltabdesc = Pdxltabdesc(popDTS->Ptabdesc(), pdrgpcrOutput);

	// construct plan costs
	CDXLPhysicalProperties *pdxlpropDTS = Pdxlprop(pexprDTS);
	
	if (NULL != pdxlprop)
	{
		CWStringDynamic *pstrRows = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp, pdxlprop->Pdxlopcost()->PstrRows()->Wsz());
		CWStringDynamic *pstrCost = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp, pdxlprop->Pdxlopcost()->PstrTotalCost()->Wsz());

		pdxlpropDTS->Pdxlopcost()->SetRows(pstrRows);
		pdxlpropDTS->Pdxlopcost()->SetCost(pstrCost);
		pdxlprop->Release();
	}

	// construct dynamic table scan operator
	CDXLPhysicalDynamicTableScan *pdxlopDTS = 
			GPOS_NEW(m_pmp) CDXLPhysicalDynamicTableScan
						(
						m_pmp, 
						pdxltabdesc, 
						popDTS->UlSecondaryScanId(),
						popDTS->UlScanId()
						);

	CDXLNode *pdxlnDTS = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopDTS);
	pdxlnDTS->SetProperties(pdxlpropDTS);
	
	CDXLNode *pdxlnCond = NULL;
	
	if (NULL != pexprScalarCond)
	{
		pdxlnCond = PdxlnScalar(pexprScalarCond);
	}
	
	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnCond);
	
	// construct projection list
	GPOS_ASSERT(NULL != pexprDTS->Prpp());
	
	CColRefSet *pcrsOutput = pexprDTS->Prpp()->PcrsRequired();
	pdxlnDTS->AddChild(PdxlnProjList(pcrsOutput, pdrgpcr));
	pdxlnDTS->AddChild(pdxlnFilter);
	
#ifdef GPOS_DEBUG
	pdxlnDTS->Pdxlop()->AssertValid(pdxlnDTS, false /* fValidateChildren */);
#endif
	
	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(pexprDTS->Pdp(CDrvdProp::EptPlan))->Pds();
	pds->AddRef();
	pdrgpdsBaseTables->Append(pds);
	
	return pdxlnDTS;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnDynamicBitmapTableScan
//
//	@doc:
//		Create a DXL dynamic bitmap table scan node from an optimizer
//		dynamic bitmap table scan node.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnDynamicBitmapTableScan
	(
	CExpression *pexprScan,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	BOOL * // pfDML
	)
{
	CExpression *pexprScalar = NULL;
	CDXLPhysicalProperties *pdxlprop = NULL;
	return PdxlnDynamicBitmapTableScan
			(
			pexprScan,
			pdrgpcr,
			pdrgpdsBaseTables,
			pexprScalar,
			pdxlprop
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnDynamicBitmapTableScan
//
//	@doc:
//		Create a DXL dynamic bitmap table scan node from an optimizer
//		dynamic bitmap table scan node.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnDynamicBitmapTableScan
	(
	CExpression *pexprScan,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	CExpression *pexprScalar,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexprScan);

	CPhysicalDynamicBitmapTableScan *pop = CPhysicalDynamicBitmapTableScan::PopConvert(pexprScan->Pop());
	DrgPcr *pdrgpcrOutput = pop->PdrgpcrOutput();

	CDXLTableDescr *pdxltabdesc = Pdxltabdesc(pop->Ptabdesc(), pdrgpcrOutput);
	CDXLPhysicalDynamicBitmapTableScan *pdxlopScan =
			GPOS_NEW(m_pmp) CDXLPhysicalDynamicBitmapTableScan
						(
						m_pmp,
						pdxltabdesc,
						pop->UlSecondaryScanId(),
						pop->UlScanId()
						);

	CDXLNode *pdxlnScan = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopScan);

	// construct plan costs
	if (NULL == pdxlprop)
	{
		pdxlprop = Pdxlprop(pexprScan);
	}
	pdxlnScan->SetProperties(pdxlprop);

	// translate predicates into DXL filter
	CDXLNode *pdxlnCond = NULL;
	if (NULL != pexprScalar)
	{
		pdxlnCond = PdxlnScalar(pexprScalar);
	}
	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnCond);

	CExpression *pexprRecheckCond = (*pexprScan)[0];
	CDXLNode *pdxlnRecheckCond = PdxlnScalar(pexprRecheckCond);
	CDXLNode *pdxlnRecheckCondFilter =
			GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarRecheckCondFilter(m_pmp));
	pdxlnRecheckCondFilter->AddChild(pdxlnRecheckCond);

	// translate bitmap access path
	CDXLNode *pdxlnBitmapAccessPath = PdxlnScalar((*pexprScan)[1]);

	// build projection list
	CColRefSet *pcrsOutput = pexprScan->Prpp()->PcrsRequired();
	AddBitmapFilterColumns(m_pmp, pop, pexprRecheckCond, pexprScalar, pcrsOutput);
	CDXLNode *pdxlnProjList = PdxlnProjList(pcrsOutput, pdrgpcr);

	pdxlnScan->AddChild(pdxlnProjList);
	pdxlnScan->AddChild(pdxlnFilter);
	pdxlnScan->AddChild(pdxlnRecheckCondFilter);
	pdxlnScan->AddChild(pdxlnBitmapAccessPath);

#ifdef GPOS_DEBUG
	pdxlnScan->Pdxlop()->AssertValid(pdxlnScan, false /* fValidateChildren */);
#endif

	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(pexprScan->Pdp(CDrvdProp::EptPlan))->Pds();
	pds->AddRef();
	pdrgpdsBaseTables->Append(pds);
	
	return pdxlnScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnDynamicIndexScan
//
//	@doc:
//		Create a DXL dynamic index scan node from an optimizer 
//		dynamic index scan node based on passed properties
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnDynamicIndexScan
	(
	CExpression *pexprDIS,
	DrgPcr *pdrgpcr,
	CDXLPhysicalProperties *pdxlprop,
	CReqdPropPlan *prpp
	)
{
	GPOS_ASSERT(NULL != pexprDIS);
	GPOS_ASSERT(NULL != pdxlprop);
	GPOS_ASSERT(NULL != prpp);

	CPhysicalDynamicIndexScan *popDIS = CPhysicalDynamicIndexScan::PopConvert(pexprDIS->Pop());	
	DrgPcr *pdrgpcrOutput = popDIS->PdrgpcrOutput();
	
	// translate table descriptor
	CDXLTableDescr *pdxltabdesc = Pdxltabdesc(popDIS->Ptabdesc(), pdrgpcrOutput);

	// create index descriptor
	CIndexDescriptor *pindexdesc = popDIS->Pindexdesc();
	CMDName *pmdnameIndex = GPOS_NEW(m_pmp) CMDName(m_pmp, pindexdesc->Name().Pstr());
	IMDId *pmdidIndex = pindexdesc->Pmdid();
	pmdidIndex->AddRef();
	CDXLIndexDescr *pdxlid = GPOS_NEW(m_pmp) CDXLIndexDescr(m_pmp, pmdidIndex, pmdnameIndex);

	// TODO: vrgahavan; we assume that the index are always forward access.
	// create the physical index scan operator
	CDXLNode *pdxlnDIS = GPOS_NEW(m_pmp) CDXLNode
									(
									m_pmp, 
									GPOS_NEW(m_pmp) CDXLPhysicalDynamicIndexScan
													(
													m_pmp,
													pdxltabdesc,
													popDIS->UlSecondaryScanId(),
													popDIS->UlScanId(),
													pdxlid,
													EdxlisdForward
													)
									);
	
	// set plan costs
	pdxlnDIS->SetProperties(pdxlprop);
	
	// construct projection list
	CColRefSet *pcrsOutput = prpp->PcrsRequired();
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcr);
	
	// translate index predicates
	CExpression *pexprCond = (*pexprDIS)[0];
	CDXLNode *pdxlnIndexCondList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarIndexCondList(m_pmp));

	DrgPexpr *pdrgpexprConds = CPredicateUtils::PdrgpexprConjuncts(m_pmp, pexprCond);
	const ULONG ulLength = pdrgpexprConds->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		CExpression *pexprIndexCond = (*pdrgpexprConds)[ul];
		CDXLNode *pdxlnIndexCond = PdxlnScalar(pexprIndexCond);
		pdxlnIndexCondList->AddChild(pdxlnIndexCond);
	}
	pdrgpexprConds->Release();

	CDXLNode *pdxlnResidualCond = NULL;
	if (2 == pexprDIS->UlArity())
	{
		// translate residual predicates into the filter node
		CExpression *pexprResidualCond = (*pexprDIS)[1];
		if (COperator::EopScalarConst != pexprResidualCond->Pop()->Eopid())
		{
			pdxlnResidualCond = PdxlnScalar(pexprResidualCond);
		}
	}

	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnResidualCond);

	pdxlnDIS->AddChild(pdxlnPrL);
	pdxlnDIS->AddChild(pdxlnFilter);
	pdxlnDIS->AddChild(pdxlnIndexCondList);
	
#ifdef GPOS_DEBUG
	pdxlnDIS->Pdxlop()->AssertValid(pdxlnDIS, false /* fValidateChildren */);
#endif
		
	return pdxlnDIS;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnDynamicIndexScan
//
//	@doc:
//		Create a DXL dynamic index scan node from an optimizer
//		dynamic index scan node.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnDynamicIndexScan
	(
	CExpression *pexprDIS,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	BOOL * // pfDML
	)
{
	GPOS_ASSERT(NULL != pexprDIS);

	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprDIS);

	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(pexprDIS->Pdp(CDrvdProp::EptPlan))->Pds();
	pds->AddRef();
	pdrgpdsBaseTables->Append(pds);
	return PdxlnDynamicIndexScan(pexprDIS, pdrgpcr, pdxlprop, pexprDIS->Prpp());
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnResult
//
//	@doc:
//		Create a DXL result node over a relational expression with a DXL
//		scalar condition.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnResult
	(
	CExpression *pexprRelational,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	CDXLNode *pdxlnScalar
	)
{
	// extract physical properties from filter
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprRelational);

	return PdxlnResult(pexprRelational, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, pdxlnScalar, pdxlprop);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnResult
//
//	@doc:
//		Create a DXL result node over a relational expression with a DXL
//		scalar condition using the passed DXL properties
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnResult
	(
	CExpression *pexprRelational,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	CDXLNode *pdxlnScalar,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexprRelational);
	GPOS_ASSERT(NULL != pdxlprop);

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprRelational, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot */);

	// wrap condition in a DXL filter node
	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnScalar);

	GPOS_ASSERT(NULL != pexprRelational->Prpp());
	CColRefSet *pcrsOutput = pexprRelational->Prpp()->PcrsRequired();
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcr);

	// create an empty one-time filter
	CDXLNode *pdxlnOneTimeFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp));

	return CTranslatorExprToDXLUtils::PdxlnResult
										(
										m_pmp,
										pdxlprop,
										pdxlnPrL,
										pdxlnFilter,
										pdxlnOneTimeFilter,
										pdxlnChild
										);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnResult
//
//	@doc:
//		Create a DXL result node from an optimizer filter node.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnResult
	(
	CExpression *pexprFilter,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprFilter);

	CDXLNode *pdxlnode = PdxlnResult(pexprFilter, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, pdxlprop);
	pdxlprop->Release();

	return pdxlnode;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnIndexScanWithInlinedCondition
//
//	@doc:
//		Create a (dynamic) index scan node after inlining the given
//		scalar condition, if needed
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnIndexScanWithInlinedCondition
	(
	CExpression *pexprIndexScan,
	CExpression *pexprScalarCond,
	CDXLPhysicalProperties *pdxlprop,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables
	)
{
	GPOS_ASSERT(NULL != pexprIndexScan);
	GPOS_ASSERT(NULL != pexprScalarCond);
	GPOS_ASSERT(pexprScalarCond->Pop()->FScalar());

	COperator::EOperatorId eopid = pexprIndexScan->Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalIndexScan == eopid ||
			COperator::EopPhysicalDynamicIndexScan == eopid);

	// inline scalar condition in index scan, if it is not the same as index lookup condition
	CExpression *pexprIndexLookupCond = (*pexprIndexScan)[0];
	CDXLNode *pdxlnIndexScan = NULL;
	if (!CUtils::FScalarConstTrue(pexprScalarCond) && !pexprScalarCond->FMatch(pexprIndexLookupCond))
	{
		// combine scalar condition with existing index conditions, if any
		pexprScalarCond->AddRef();
		CExpression *pexprNewScalarCond = pexprScalarCond;
		if (2 == pexprIndexScan->UlArity())
		{
			pexprNewScalarCond->Release();
			pexprNewScalarCond = CPredicateUtils::PexprConjunction(m_pmp, (*pexprIndexScan)[1], pexprScalarCond);
		}
		pexprIndexLookupCond->AddRef();
		pexprIndexScan->Pop()->AddRef();
		CExpression *pexprNewIndexScan = GPOS_NEW(m_pmp) CExpression(m_pmp, pexprIndexScan->Pop(), pexprIndexLookupCond, pexprNewScalarCond);
		if (COperator::EopPhysicalIndexScan == eopid)
		{
			pdxlnIndexScan = PdxlnIndexScan(pexprNewIndexScan, pdrgpcr, pdxlprop, pexprIndexScan->Prpp());
		}
		else
		{
			pdxlnIndexScan = PdxlnDynamicIndexScan(pexprNewIndexScan, pdrgpcr, pdxlprop, pexprIndexScan->Prpp());
		}
		pexprNewIndexScan->Release();

		CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(pexprIndexScan->Pdp(CDrvdProp::EptPlan))->Pds();
		pds->AddRef();
		pdrgpdsBaseTables->Append(pds);
		
		return pdxlnIndexScan;
	}

	// index scan does not need the properties of the filter, as it does not
	// need to further inline the scalar condition
	pdxlprop->Release();
	ULONG ulNonGatherMotions = 0;
	BOOL fDML = false;
	if (COperator::EopPhysicalIndexScan == eopid)
	{
		return PdxlnIndexScan(pexprIndexScan, pdrgpcr, pdrgpdsBaseTables, &ulNonGatherMotions, &fDML);
	}

	return PdxlnDynamicIndexScan(pexprIndexScan, pdrgpcr, pdrgpdsBaseTables, &ulNonGatherMotions, &fDML);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnResult
//
//	@doc:
//		Create a DXL result node from an optimizer filter node.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnResult
	(
	CExpression *pexprFilter,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexprFilter);
	GPOS_ASSERT(NULL != pdxlprop);

	// extract components
	CExpression *pexprRelational = (*pexprFilter)[0];
	CExpression *pexprScalar = (*pexprFilter)[1];

	// if the filter predicate is a constant TRUE, skip to translating relational child
	if (CUtils::FScalarConstTrue(pexprScalar))
	{
		return Pdxln(pexprRelational, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, true /*fRemap*/, false /* fRoot */);
	}

	COperator::EOperatorId eopidRelational = pexprRelational->Pop()->Eopid();
	CColRefSet *pcrsOutput = pexprFilter->Prpp()->PcrsRequired();

	switch (eopidRelational)
	{
		case COperator::EopPhysicalTableScan:
		case COperator::EopPhysicalExternalScan:
		{
			// if there is a structure of the form
			// 		filter->tablescan, or filter->CTG then
			// push the scalar filter expression to the tablescan/CTG respectively
			pdxlprop->AddRef();

			// translate the table scan with the filter condition
			return PdxlnTblScan
					(
					pexprRelational,
					pcrsOutput,
					NULL /* pdrgpcr */,
					pdrgpdsBaseTables, 
					pexprScalar,
					pdxlprop /* cost info */
					);
		}
		case COperator::EopPhysicalBitmapTableScan:
		{
			pdxlprop->AddRef();

			return PdxlnBitmapTableScan
					(
					pexprRelational,
					pcrsOutput,
					NULL /*pdrgpcr*/,
					pdrgpdsBaseTables,
					pexprScalar,
					pdxlprop
					);
		}
		case COperator::EopPhysicalDynamicTableScan:
		{
			pdxlprop->AddRef();

			// inline condition in the Dynamic Table Scan
			return PdxlnDynamicTableScan(pexprRelational, pdrgpcr, pdrgpdsBaseTables, pexprScalar, pdxlprop);
		}
		case COperator::EopPhysicalIndexScan:
		case COperator::EopPhysicalDynamicIndexScan:
		{
			return PdxlnIndexScanWithInlinedCondition
						(
						pexprRelational, 
						pexprScalar,
						Pdxlprop(pexprFilter), 
						pdrgpcr,
						pdrgpdsBaseTables
						);
		}
		case COperator::EopPhysicalDynamicBitmapTableScan:
		{
			pdxlprop->AddRef();

			return PdxlnDynamicBitmapTableScan(pexprRelational, pdrgpcr, pdrgpdsBaseTables, pexprScalar, pdxlprop);
		}
		default:
		{
			return PdxlnResultFromFilter(pexprFilter, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
		}
	}
	
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPartitionSelectorWithInlinedCondition
//
//	@doc:
//		Translate a partition selector into DXL while inlining the given
//		condition in the child
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPartitionSelectorWithInlinedCondition
	(
	CExpression *pexprFilter,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables,
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprFilter);
	GPOS_ASSERT(COperator::EopPhysicalFilter == pexprFilter->Pop()->Eopid());
	GPOS_ASSERT(COperator::EopPhysicalPartitionSelector == (*pexprFilter)[0]->Pop()->Eopid());

	CExpression *pexprRelational = (*pexprFilter)[0];
	CExpression *pexprScalar = (*pexprFilter)[1];
	CExpression *pexprChild = (*pexprRelational)[0];
	COperator::EOperatorId eopid = pexprChild->Pop()->Eopid();
	BOOL fTableScanChild = (COperator::EopPhysicalDynamicTableScan == eopid);
	BOOL fIndexChild = (COperator::EopPhysicalDynamicIndexScan == eopid || COperator::EopPhysicalDynamicBitmapTableScan == eopid);
	GPOS_ASSERT(fTableScanChild || fIndexChild);

	// inline condition in child operator if the following conditions are met:
	BOOL fInlineCondition =
		NULL != pexprScalar &&	// condition is not NULL
		!CUtils::FScalarConstTrue(pexprScalar) &&	// condition is not const True
		(
		fTableScanChild || 	// child operator is TableScan
		(fIndexChild && !pexprScalar->FMatch((*pexprChild)[0]))	// OR, child operator is IndexScan and condition does not match index condition
		);

	CExpression *pexprCond = NULL;
	CDXLPhysicalProperties *pdxlprop = NULL;
	if (fInlineCondition)
	{
		pexprCond = pexprScalar;
		pdxlprop = Pdxlprop(pexprFilter);
	}

	return PdxlnPartitionSelector(pexprRelational, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, pexprCond, pdxlprop);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnResultFromFilter
//
//	@doc:
//		Create a DXL result node from an optimizer filter node.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnResultFromFilter
	(
	CExpression *pexprFilter,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprFilter);

	// extract components
	CExpression *pexprRelational = (*pexprFilter)[0];
	CExpression *pexprScalar = (*pexprFilter)[1];
	CColRefSet *pcrsOutput = pexprFilter->Prpp()->PcrsRequired();

	if (COperator::EopPhysicalPartitionSelector == pexprRelational->Pop()->Eopid())
	{
		COperator::EOperatorId eopid = (*pexprRelational)[0]->Pop()->Eopid();
		if (COperator::EopPhysicalDynamicIndexScan == eopid ||
			COperator::EopPhysicalDynamicBitmapTableScan == eopid ||
			COperator::EopPhysicalDynamicTableScan == eopid)
		{
			return PdxlnPartitionSelectorWithInlinedCondition(pexprFilter, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
		}
	}

	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprFilter);

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprRelational, NULL /* pdrgpcr */, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	// translate scalar expression
	CDXLNode *pdxlnCond = PdxlnScalar(pexprScalar);

	// wrap condition in a DXL filter node
	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnCond);

	GPOS_ASSERT(NULL != pexprFilter->Prpp());

	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcr);

	// create an empty one-time filter
	CDXLNode *pdxlnOneTimeFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp));

	return CTranslatorExprToDXLUtils::PdxlnResult
											(
											m_pmp,
											pdxlprop,
											pdxlnPrL,
											pdxlnFilter,
											pdxlnOneTimeFilter,
											pdxlnChild
											);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnAssert
//
//	@doc:
//		Translate a physical assert expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnAssert
	(
	CExpression *pexprAssert,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprAssert);

	// extract components
	CExpression *pexprRelational = (*pexprAssert)[0];
	CExpression *pexprScalar = (*pexprAssert)[1];
	CPhysicalAssert *popAssert = CPhysicalAssert::PopConvert(pexprAssert->Pop());
	
	// extract physical properties from assert
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprAssert);

	CColRefSet *pcrsOutput = pexprAssert->Prpp()->PcrsRequired();

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprRelational, NULL /* pdrgpcr */, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	// translate scalar expression
	CDXLNode *pdxlnAssertPredicate = PdxlnScalar(pexprScalar);

	GPOS_ASSERT(NULL != pexprAssert->Prpp());

	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcr);

	const CHAR *szSQLState = popAssert->Pexc()->SzSQLState();
	CDXLPhysicalAssert *pdxlopAssert = GPOS_NEW(m_pmp) CDXLPhysicalAssert(m_pmp, szSQLState);
	CDXLNode *pdxlnAssert = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopAssert, pdxlnPrL, pdxlnAssertPredicate, pdxlnChild);
	
	pdxlnAssert->SetProperties(pdxlprop);
	
	return pdxlnAssert;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnCTEProducer
//
//	@doc:
//		Translate a physical cte producer expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnCTEProducer
	(
	CExpression *pexprCTEProducer,
	DrgPcr * , //pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprCTEProducer);

	// extract components
	CExpression *pexprRelational = (*pexprCTEProducer)[0];
	CPhysicalCTEProducer *popCTEProducer = CPhysicalCTEProducer::PopConvert(pexprCTEProducer->Pop());

	// extract physical properties from cte producer
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprCTEProducer);

	// extract the CTE id and the array of colids
	const ULONG ulCTEId = popCTEProducer->UlCTEId();
	DrgPul *pdrgpulColIds = CUtils::Pdrgpul(m_pmp, popCTEProducer->Pdrgpcr());

	GPOS_ASSERT(NULL != pexprCTEProducer->Prpp());
	DrgPcr *pdrgpcrRequired = popCTEProducer->Pdrgpcr();
	CColRefSet *pcrsOutput = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrsOutput->Include(pdrgpcrRequired);

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprRelational, pdrgpcrRequired, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, true /*fRemap*/, false /*fRoot */);

	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcrRequired);
	pcrsOutput->Release();

	CDXLNode *pdxlnCTEProducer = GPOS_NEW(m_pmp) CDXLNode
										(
										m_pmp,
										GPOS_NEW(m_pmp) CDXLPhysicalCTEProducer(m_pmp, ulCTEId, pdrgpulColIds),
										pdxlnPrL,
										pdxlnChild
										);

	pdxlnCTEProducer->SetProperties(pdxlprop);

	return pdxlnCTEProducer;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnCTEConsumer
//
//	@doc:
//		Translate a physical cte consumer expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnCTEConsumer
	(
	CExpression *pexprCTEConsumer,
	DrgPcr *, //pdrgpcr,
	DrgPds *, // pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	BOOL * // pfDML
	)
{
	GPOS_ASSERT(NULL != pexprCTEConsumer);

	// extract components
	CPhysicalCTEConsumer *popCTEConsumer = CPhysicalCTEConsumer::PopConvert(pexprCTEConsumer->Pop());

	// extract physical properties from cte consumer
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprCTEConsumer);

	// extract the CTE id and the array of colids
	const ULONG ulCTEId = popCTEConsumer->UlCTEId();
	DrgPcr *pdrgpcr = popCTEConsumer->Pdrgpcr();
	DrgPul *pdrgpulColIds = CUtils::Pdrgpul(m_pmp, pdrgpcr);

	CColRefSet *pcrsOutput = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrsOutput->Include(pdrgpcr);

	// translate relational child expression
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcr);

	CDXLNode *pdxlnCTEConsumer = GPOS_NEW(m_pmp) CDXLNode
										(
										m_pmp,
										GPOS_NEW(m_pmp) CDXLPhysicalCTEConsumer(m_pmp, ulCTEId, pdrgpulColIds),
										pdxlnPrL
										);

	pcrsOutput->Release();

	pdxlnCTEConsumer->SetProperties(pdxlprop);

	return pdxlnCTEConsumer;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnAppend
//
//	@doc:
//		Create a DXL Append node from an optimizer an union all node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnAppend
	(
	CExpression *pexprUnionAll,
	DrgPcr *, //pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprUnionAll);

	CPhysicalUnionAll *popUnionAll = CPhysicalUnionAll::PopConvert(pexprUnionAll->Pop());
	DrgPcr *pdrgpcrOutput = popUnionAll->PdrgpcrOutput();

	CDXLPhysicalAppend *pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalAppend(m_pmp, false, false);
	CDXLNode *pdxlnAppend = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// set properties
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprUnionAll);
	pdxlnAppend->SetProperties(pdxlprop);

	// translate project list
	CColRefSet *pcrsOutput = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrsOutput->Include(pdrgpcrOutput);

	// the append node does not re-order or trim it input or output columns. The trimming
	// and re-ordering of its output columns has to be done above it (if needed)
	// via a separate result node
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcrOutput);
	pcrsOutput->Release();
	pcrsOutput = NULL;

	pdxlnAppend->AddChild(pdxlnPrL);

	// scalar condition
	CDXLNode *pdxlnCond = NULL;
	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnCond);
	pdxlnAppend->AddChild(pdxlnFilter);

	// translate children
	DrgDrgPcr *pdrgpdrgpcrInput = popUnionAll->PdrgpdrgpcrInput();
	GPOS_ASSERT(NULL != pdrgpdrgpcrInput);
	const ULONG ulLen = pexprUnionAll->UlArity();
	GPOS_ASSERT(ulLen == pdrgpdrgpcrInput->UlLength());
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		// translate child
		DrgPcr *pdrgpcrInput = (*pdrgpdrgpcrInput)[ul];

		CExpression *pexprChild = (*pexprUnionAll)[ul];
		CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcrInput, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

		// add a result node on top if necessary so the order of the input project list
		// matches the order in which the append node requires it
		CDXLNode *pdxlnChildProjected = PdxlnRemapOutputColumns
											(
											pexprChild,
											pdxlnChild,
											pdrgpcrInput /* required input columns */,
											pdrgpcrInput /* order of the input columns */
											);

		pdxlnAppend->AddChild(pdxlnChildProjected);
	}

	return pdxlnAppend;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdrgpcrMerge
//
//	@doc: 
//		Combines the ordered columns and required columns into a single list
//      with members in the ordered list inserted before the remaining columns in 
//		required list. For instance, if the order list is (c, d) and 
//		the required list is (a, b, c, d) then the combined list is (c, d, a, b)
//---------------------------------------------------------------------------
DrgPcr *
CTranslatorExprToDXL::PdrgpcrMerge
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOrder,
	DrgPcr *pdrgpcrRequired
	)
{
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);

	DrgPcr *pdrgpcrMerge = GPOS_NEW(pmp) DrgPcr(pmp);

	if (NULL != pdrgpcrOrder)
	{
		const ULONG ulLenOrder = pdrgpcrOrder->UlLength();
		for (ULONG ul = 0; ul < ulLenOrder; ul++)
		{
			CColRef *pcr = (*pdrgpcrOrder)[ul];
			pdrgpcrMerge->Append(pcr);
		}
		pcrsOutput->Include(pdrgpcrMerge);
	}

	const ULONG ulLenReqd = pdrgpcrRequired->UlLength();
	for (ULONG ul = 0; ul < ulLenReqd; ul++)
	{
		CColRef *pcr = (*pdrgpcrRequired)[ul];
		if (!pcrsOutput->FMember(pcr))
		{
			pcrsOutput->Include(pcr);
			pdrgpcrMerge->Append(pcr);
		}
	}
	
	pcrsOutput->Release();

	return pdrgpcrMerge;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnRemapOutputColumns
//
//	@doc:
//		Checks if the project list of the given node matches the required
//		columns and their order. If not then a result node is created on
//		top of it
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnRemapOutputColumns
	(
	CExpression *pexpr,
	CDXLNode *pdxln,
	DrgPcr *pdrgpcrRequired,
	DrgPcr *pdrgpcrOrder
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pdxln);
	GPOS_ASSERT(NULL != pdrgpcrRequired);

	// get project list
	CDXLNode *pdxlnPrL = (*pdxln)[0];

	DrgPcr *pdrgpcrOrderedReqdCols = PdrgpcrMerge(m_pmp, pdrgpcrOrder, pdrgpcrRequired);
	
	// if the combined list is the same as proj list then no
	// further action needed. Otherwise we need result node on top
	if (CTranslatorExprToDXLUtils::FProjectListMatch(pdxlnPrL, pdrgpcrOrderedReqdCols))
	{
		pdrgpcrOrderedReqdCols->Release();
		return pdxln;
	}

	pdrgpcrOrderedReqdCols->Release();
	
	// output columns of new result node
	CColRefSet *pcrsOutput = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrsOutput->Include(pdrgpcrRequired);

	CDXLNode *pdxlnPrLNew = PdxlnProjList(pcrsOutput, pdrgpcrOrder);
	pcrsOutput->Release();

	// empty one time filter
	CDXLNode *pdxlnOneTimeFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp));

	CDXLNode *pdxlnResult = CTranslatorExprToDXLUtils::PdxlnResult
														(
														m_pmp,
														Pdxlprop(pexpr),
														pdxlnPrLNew,
														PdxlnFilter(NULL),
														pdxlnOneTimeFilter,
														pdxln
														);

	return pdxlnResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnTVF
//
//	@doc:
//		Create a DXL TVF node from an optimizer TVF node
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnTVF
	(
	CExpression *pexprTVF,
	DrgPcr *, //pdrgpcr,
	DrgPds *, // pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	BOOL * // pfDML
	)
{
	GPOS_ASSERT(NULL != pexprTVF);

	CPhysicalTVF *popTVF = CPhysicalTVF::PopConvert(pexprTVF->Pop());

	CColRefSet *pcrsOutput = popTVF->PcrsOutput();

	IMDId *pmdidFunc = popTVF->PmdidFunc();
	pmdidFunc->AddRef();

	IMDId *pmdidRetType = popTVF->PmdidRetType();
	pmdidRetType->AddRef();

	CWStringConst *pstrFunc = GPOS_NEW(m_pmp) CWStringConst(m_pmp, popTVF->Pstr()->Wsz());

	CDXLPhysicalTVF *pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalTVF(m_pmp, pmdidFunc, pmdidRetType, pstrFunc);

	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprTVF);
	CDXLNode *pdxlnTVF = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	pdxlnTVF->SetProperties(pdxlprop);
	
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, NULL /*pdrgpcr*/);
	pdxlnTVF->AddChild(pdxlnPrL); 		// project list

	TranslateScalarChildren(pexprTVF, pdxlnTVF);

	return pdxlnTVF;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnResultFromConstTableGet
//
//	@doc:
//		Create a DXL result node from an optimizer const table get node
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnResultFromConstTableGet
	(
	CExpression *pexprCTG,
	DrgPcr *pdrgpcr,
	CExpression *pexprScalar
	)
{
	GPOS_ASSERT(NULL != pexprCTG);
	
	CPhysicalConstTableGet *popCTG = CPhysicalConstTableGet::PopConvert(pexprCTG->Pop()); 
	
	// construct project list from the const table get values
	DrgPcr *pdrgpcrCTGOutput = popCTG->PdrgpcrOutput();
	DrgPdrgPdatum *pdrgpdrgdatum = popCTG->Pdrgpdrgpdatum();
	
	const ULONG ulRows = pdrgpdrgdatum->UlLength();
	CDXLNode *pdxlnPrL = NULL;
	CDXLNode *pdxlnOneTimeFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp));

	DrgPdatum *pdrgpdatum = NULL;
	if (0 == ulRows)
	{
		// no-tuples... only generate one row of NULLS and one-time "false" filter
		pdrgpdatum = CTranslatorExprToDXLUtils::PdrgpdatumNulls(m_pmp, pdrgpcrCTGOutput);

		CExpression *pexprFalse = CUtils::PexprScalarConstBool(m_pmp, false /*fVal*/, false /*fNull*/);
		CDXLNode *pdxlnFalse = PdxlnScConst(pexprFalse);
		pexprFalse->Release();

		pdxlnOneTimeFilter->AddChild(pdxlnFalse);
	}
	else
	{
		// TODO:  - Feb 29, 2012; add support for CTGs with multiple rows
		GPOS_ASSERT(1 == ulRows);
		pdrgpdatum = (*pdrgpdrgdatum)[0];
		pdrgpdatum->AddRef();
		CDXLNode *pdxlnCond = NULL;
		if (NULL != pexprScalar)
		{
			pdxlnCond = PdxlnScalar(pexprScalar);
			pdxlnOneTimeFilter->AddChild(pdxlnCond);
		}
	}

	pdxlnPrL = PdxlnProjListFromConstTableGet(pdrgpcr, pdrgpcrCTGOutput, pdrgpdatum);
	pdrgpdatum->Release();

	return CTranslatorExprToDXLUtils::PdxlnResult
										(
										m_pmp,
										Pdxlprop(pexprCTG),
										pdxlnPrL,
										PdxlnFilter(NULL),
										pdxlnOneTimeFilter,
										NULL //pdxlnChild
										);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnResultFromConstTableGet
//
//	@doc:
//		Create a DXL result node from an optimizer const table get node
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnResultFromConstTableGet
	(
	CExpression *pexprCTG,
	DrgPcr *pdrgpcr,
	DrgPds *, // pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	BOOL * // pfDML
	)
{
	return PdxlnResultFromConstTableGet(pexprCTG, pdrgpcr, NULL /*pexprScalarCond*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnComputeScalar
//
//	@doc:
//		Create a DXL result node from an optimizer compute scalar expression
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnComputeScalar
	(
	CExpression *pexprComputeScalar,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprComputeScalar);

	// extract components
	CExpression *pexprRelational = (*pexprComputeScalar)[0];
	CExpression *pexprProjList = (*pexprComputeScalar)[1];

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprRelational, NULL /* pdrgpcr */, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/ );

	// compute required columns
	GPOS_ASSERT(NULL != pexprComputeScalar->Prpp());
	CColRefSet *pcrsOutput = pexprComputeScalar->Prpp()->PcrsRequired();

	// iterate the columns in the projection list, add the columns containing
	// set-returning functions to the output columns
	const ULONG ulPrLs = pexprProjList->UlArity();
	for (ULONG ul = 0; ul < ulPrLs; ul++)
	{
		CExpression *pexprPrE = (*pexprProjList)[ul];
		CDrvdPropScalar *pdpscalar = CDrvdPropScalar::Pdpscalar(pexprPrE->PdpDerive());

		// for column that doesn't contain set-returning function, if it is not the
		// required column in the relational plan properties, then no need to add them
		// to the output columns
		if (pdpscalar->FHasNonScalarFunction())
		{
			CScalarProjectElement *popScPrE = CScalarProjectElement::PopConvert(pexprPrE->Pop());
			pcrsOutput->Include(popScPrE->Pcr());
		}
	}

	// translate project list expression
	CDXLNode *pdxlnPrL = NULL;
	if (NULL == pdrgpcr || CUtils::FHasDuplicates(pdrgpcr))
	{
		pdxlnPrL = PdxlnProjList(pexprProjList, pcrsOutput);
	}
	else
	{
		pdxlnPrL = PdxlnProjList(pexprProjList, pcrsOutput, pdrgpcr);
	}

	// construct an empty plan filter
	CDXLNode *pdxlnFilter = PdxlnFilter(NULL);

	// create an empty one-time filter
	CDXLNode *pdxlnOneTimeFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp));

	// construct a Result node
	CDXLPhysicalResult *pdxlopResult = GPOS_NEW(m_pmp) CDXLPhysicalResult(m_pmp);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprComputeScalar);	
	CDXLNode *pdxlnResult = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopResult);
	pdxlnResult->SetProperties(pdxlprop);
	
	// add children
	pdxlnResult->AddChild(pdxlnPrL);
	pdxlnResult->AddChild(pdxlnFilter);
	pdxlnResult->AddChild(pdxlnOneTimeFilter);
	pdxlnResult->AddChild(pdxlnChild);

#ifdef GPOS_DEBUG
	(void) pdxlopResult->AssertValid(pdxlnResult, false /* fValidateChildren */);
#endif
	return pdxlnResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnAggregate
//
//	@doc:
//		Create a DXL aggregate node from an optimizer hash agg expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnAggregate
	(
	CExpression *pexprAgg,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprAgg);
	COperator::EOperatorId eopid = pexprAgg->Pop()->Eopid();
	
	// extract components and construct an aggregate node
	CPhysicalAgg *popAgg = NULL;

	GPOS_ASSERT(COperator::EopPhysicalStreamAgg == eopid ||
				COperator::EopPhysicalHashAgg == eopid ||
				COperator::EopPhysicalScalarAgg == eopid);

	EdxlAggStrategy edxlaggstr = EdxlaggstrategySentinel;

	switch (eopid)
	{
		case COperator::EopPhysicalStreamAgg:
						{
							popAgg = CPhysicalStreamAgg::PopConvert(pexprAgg->Pop());
							edxlaggstr = EdxlaggstrategySorted;
							break;
						}
		case COperator::EopPhysicalHashAgg:
						{
							popAgg = CPhysicalHashAgg::PopConvert(pexprAgg->Pop());
							edxlaggstr = EdxlaggstrategyHashed;
							break;
						}
		case COperator::EopPhysicalScalarAgg:
						{
							popAgg = CPhysicalScalarAgg::PopConvert(pexprAgg->Pop());
							edxlaggstr = EdxlaggstrategyPlain;
							break;
						}
		default:
			{
				return NULL;	// to silence the compiler
			}
	}

	const DrgPcr *pdrgpcrGroupingCols = popAgg->PdrgpcrGroupingCols();

	return PdxlnAggregate
			(
			pexprAgg,
			pdrgpcr, 
			pdrgpdsBaseTables, 
			pulNonGatherMotions,
			pfDML,
			edxlaggstr,
			pdrgpcrGroupingCols,
			NULL /*pcrsKeys*/
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnAggregateDedup
//
//	@doc:
//		Create a DXL aggregate node from an optimizer dedup agg expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnAggregateDedup
	(
	CExpression *pexprAgg,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprAgg);
	COperator::EOperatorId eopid = pexprAgg->Pop()->Eopid();

	GPOS_ASSERT(COperator::EopPhysicalStreamAggDeduplicate == eopid ||
				COperator::EopPhysicalHashAggDeduplicate == eopid);

	EdxlAggStrategy edxlaggstr = EdxlaggstrategySentinel;
	const DrgPcr *pdrgpcrGroupingCols = NULL;
	CColRefSet *pcrsKeys = GPOS_NEW(m_pmp) CColRefSet(m_pmp);

	if (COperator::EopPhysicalStreamAggDeduplicate == eopid)
	{
		CPhysicalStreamAggDeduplicate *popAggDedup = CPhysicalStreamAggDeduplicate::PopConvert(pexprAgg->Pop());
		pcrsKeys->Include(popAggDedup->PdrgpcrKeys());
		pdrgpcrGroupingCols = popAggDedup->PdrgpcrGroupingCols();
		edxlaggstr = EdxlaggstrategySorted;
	}
	else
	{
		CPhysicalHashAggDeduplicate *popAggDedup = CPhysicalHashAggDeduplicate::PopConvert(pexprAgg->Pop());
		pcrsKeys->Include(popAggDedup->PdrgpcrKeys());
		pdrgpcrGroupingCols = popAggDedup->PdrgpcrGroupingCols();
		edxlaggstr = EdxlaggstrategyHashed;
	}

	CDXLNode *pdxlnAgg = PdxlnAggregate
							(
							pexprAgg,
							pdrgpcr,
							pdrgpdsBaseTables, 
							pulNonGatherMotions,
							pfDML,
							edxlaggstr,
							pdrgpcrGroupingCols,
							pcrsKeys
							);
	pcrsKeys->Release();

	return pdxlnAgg;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnAggregate
//
//	@doc:
//		Create a DXL aggregate node from an optimizer agg expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnAggregate
	(
	CExpression *pexprAgg,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	EdxlAggStrategy edxlaggstr,
	const DrgPcr *pdrgpcrGroupingCols,
	CColRefSet *pcrsKeys
	)
{
	GPOS_ASSERT(NULL != pexprAgg);
	GPOS_ASSERT(NULL != pdrgpcrGroupingCols);
#ifdef GPOS_DEBUG
	COperator::EOperatorId eopid = pexprAgg->Pop()->Eopid();
	GPOS_ASSERT_IMP(NULL == pcrsKeys, COperator::EopPhysicalStreamAgg == eopid ||
									COperator::EopPhysicalHashAgg == eopid ||
									COperator::EopPhysicalScalarAgg == eopid);
#endif //GPOS_DEBUG

	// is it safe to stream the local hash aggregate
	BOOL fStreamSafe = CTranslatorExprToDXLUtils::FLocalHashAggStreamSafe(pexprAgg);

	CExpression *pexprChild = (*pexprAgg)[0];
	CExpression *pexprProjList = (*pexprAgg)[1];

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln
							(
							pexprChild, 
							NULL, // pdrgpcr, 
							pdrgpdsBaseTables,  
							pulNonGatherMotions, 
							pfDML, 
							false, // fRemap, 
							false // fRoot
							);

	// compute required columns
	GPOS_ASSERT(NULL != pexprAgg->Prpp());
	CColRefSet *pcrsRequired = pexprAgg->Prpp()->PcrsRequired();

	// translate project list expression
	CDXLNode *pdxlnProjList = PdxlnProjList(pexprProjList, pcrsRequired, pdrgpcr);

	// create an empty filter
	CDXLNode *pdxlnFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarFilter(m_pmp));

	// construct grouping columns list and check if all the grouping column are
	// already in the project list of the aggregate operator

	const ULONG ulCols = pdxlnProjList->UlArity();
	HMUlUl *phmululPL = GPOS_NEW(m_pmp) HMUlUl(m_pmp);
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CDXLNode *pdxlnProjElem = (*pdxlnProjList)[ul];
		ULONG ulColId = CDXLScalarProjElem::PdxlopConvert(pdxlnProjElem->Pdxlop())->UlId();

		if (NULL == phmululPL->PtLookup(&ulColId))
		{
#ifdef GPOS_DEBUG
			BOOL fRes =
#endif
			phmululPL->FInsert(GPOS_NEW(m_pmp) ULONG(ulColId), GPOS_NEW(m_pmp) ULONG(ulColId));
			GPOS_ASSERT(fRes);
		}
	}

	DrgPul *pdrgpulGroupingCols = GPOS_NEW(m_pmp) DrgPul(m_pmp);

	const ULONG ulLen = pdrgpcrGroupingCols->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRef *pcrGroupingCol = (*pdrgpcrGroupingCols)[ul];

		// only add columns that are either required or in the join keys.
		// if the keys colrefset is null, then skip this check
		if (NULL != pcrsKeys &&
			!pcrsKeys->FMember(pcrGroupingCol) &&
			!pcrsRequired->FMember(pcrGroupingCol))
		{
			continue;
		}

		pdrgpulGroupingCols->Append(GPOS_NEW(m_pmp) ULONG(pcrGroupingCol->UlId()));

		ULONG ulColId = pcrGroupingCol->UlId();
		if (NULL == phmululPL->PtLookup(&ulColId))
		{
			CDXLNode *pdxlnProjElem = CTranslatorExprToDXLUtils::PdxlnProjElem(m_pmp, m_phmcrdxln, pcrGroupingCol);
			pdxlnProjList->AddChild(pdxlnProjElem);
#ifdef GPOS_DEBUG
		BOOL fRes =
#endif
				phmululPL->FInsert(GPOS_NEW(m_pmp) ULONG(ulColId), GPOS_NEW(m_pmp) ULONG(ulColId));
			GPOS_ASSERT(fRes);
		}
	}
	
	phmululPL->Release();

	CDXLPhysicalAgg *pdxlopAgg = GPOS_NEW(m_pmp) CDXLPhysicalAgg(m_pmp, edxlaggstr, fStreamSafe);
	pdxlopAgg->SetGroupingCols(pdrgpulGroupingCols);

	CDXLNode *pdxlnAgg = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopAgg);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprAgg);
	pdxlnAgg->SetProperties(pdxlprop);

	// add children
	pdxlnAgg->AddChild(pdxlnProjList);
	pdxlnAgg->AddChild(pdxlnFilter);
	pdxlnAgg->AddChild(pdxlnChild);

#ifdef GPOS_DEBUG
	pdxlopAgg->AssertValid(pdxlnAgg, false /* fValidateChildren */);
#endif

	return pdxlnAgg;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnSort
//
//	@doc:
//		Create a DXL sort node from an optimizer physical sort expression
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnSort
	(
	CExpression *pexprSort,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprSort);
	
	GPOS_ASSERT(1 == pexprSort->UlArity());

	// extract components
	CPhysicalSort *popSort = CPhysicalSort::PopConvert(pexprSort->Pop());
	CExpression *pexprChild = (*pexprSort)[0];

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	// translate order spec
	CDXLNode *pdxlnSortColList = PdxlnSortColList(popSort->Pos());
	
	// construct project list from child project list
	GPOS_ASSERT(NULL != pdxlnChild && 1 <= pdxlnChild->UlArity());
	CDXLNode *pdxlnProjListChild = (*pdxlnChild)[0];
	CDXLNode *pdxlnProjList = CTranslatorExprToDXLUtils::PdxlnProjListFromChildProjList(m_pmp, m_pcf, m_phmcrdxln, pdxlnProjListChild);

	// create an empty filter
	CDXLNode *pdxlnFilter = PdxlnFilter(NULL);
	
	// construct a sort node
	CDXLPhysicalSort *pdxlopSort = GPOS_NEW(m_pmp) CDXLPhysicalSort(m_pmp, false /*fDiscardDuplicates*/);
	
	// construct sort node from its components
	CDXLNode *pdxlnSort = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopSort);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprSort);
	pdxlnSort->SetProperties(pdxlprop);

	// construct empty limit count and offset nodes
	CDXLNode *pdxlnLimitCount = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarLimitCount(m_pmp));
	CDXLNode *pdxlnLimitOffset = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarLimitOffset(m_pmp));
	
	// add children
	pdxlnSort->AddChild(pdxlnProjList);
	pdxlnSort->AddChild(pdxlnFilter);
	pdxlnSort->AddChild(pdxlnSortColList);
	pdxlnSort->AddChild(pdxlnLimitCount);
	pdxlnSort->AddChild(pdxlnLimitOffset);
	pdxlnSort->AddChild(pdxlnChild);
	
#ifdef GPOS_DEBUG
	pdxlopSort->AssertValid(pdxlnSort, false /* fValidateChildren */);
#endif
	
	return pdxlnSort;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnLimit
//
//	@doc:
//		Create a DXL limit node from an optimizer physical limit expression.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnLimit
	(
	CExpression *pexprLimit,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprLimit);
	GPOS_ASSERT(3 == pexprLimit->UlArity());
	
	// extract components
	CExpression *pexprChild = (*pexprLimit)[0];
	CExpression *pexprOffset = (*pexprLimit)[1];
	CExpression *pexprCount = (*pexprLimit)[2];

	// bypass translation of limit if it does not have row count and offset
	CPhysicalLimit *popLimit = CPhysicalLimit::PopConvert(pexprLimit->Pop());
	if (!popLimit->FHasCount() && CUtils::FHasZeroOffset(pexprLimit))
	{
		return Pdxln(pexprChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, true /*fRemap*/, false /*fRoot*/);
	}
	
	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, true /*fRemap*/, false /*fRoot*/);

	// translate limit offset and count
	CDXLNode *pdxlnLimitOffset = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarLimitOffset(m_pmp));
	pdxlnLimitOffset->AddChild(PdxlnScalar(pexprOffset));
	
	CDXLNode *pdxlnLimitCount = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarLimitCount(m_pmp));
	pdxlnLimitCount->AddChild(PdxlnScalar(pexprCount));
	
	// construct project list from child project list
	GPOS_ASSERT(NULL != pdxlnChild && 1 <= pdxlnChild->UlArity());
	CDXLNode *pdxlnProjListChild = (*pdxlnChild)[0];
	CDXLNode *pdxlnProjList = CTranslatorExprToDXLUtils::PdxlnProjListFromChildProjList(m_pmp, m_pcf, m_phmcrdxln, pdxlnProjListChild);

	// construct a limit node
	CDXLPhysicalLimit *pdxlopLimit = GPOS_NEW(m_pmp) CDXLPhysicalLimit(m_pmp);

	// construct limit node from its components
	CDXLNode *pdxlnLimit = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopLimit);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprLimit);
	pdxlnLimit->SetProperties(pdxlprop);
	
	pdxlnLimit->AddChild(pdxlnProjList);
	pdxlnLimit->AddChild(pdxlnChild);
	pdxlnLimit->AddChild(pdxlnLimitCount);
	pdxlnLimit->AddChild(pdxlnLimitOffset);
	
#ifdef GPOS_DEBUG
	pdxlopLimit->AssertValid(pdxlnLimit, false /* fValidateChildren */);
#endif
	
	return pdxlnLimit;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::BuildSubplansForCorrelatedLOJ
//
//	@doc:
//		Helper to build subplans from correlated LOJ
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::BuildSubplansForCorrelatedLOJ
	(
	CExpression *pexprCorrelatedLOJ,
	DrgPdxlcr *pdrgdxlcr,
	DrgPmdid *pdrgmdid,
	CDXLNode **ppdxlnScalar, // output: scalar condition after replacing inner child reference with subplan
	DrgPds *pdrgpdsBaseTables,
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprCorrelatedLOJ);
	GPOS_ASSERT(COperator::EopPhysicalCorrelatedLeftOuterNLJoin == pexprCorrelatedLOJ->Pop()->Eopid());

	CExpression *pexprInner = (*pexprCorrelatedLOJ)[1];
	CExpression *pexprScalar = (*pexprCorrelatedLOJ)[2];

	DrgPcr *pdrgpcrInner = CPhysicalNLJoin::PopConvert(pexprCorrelatedLOJ->Pop())->PdrgPcrInner();
	GPOS_ASSERT(NULL != pdrgpcrInner);

	EdxlSubPlanType edxlsubplantype = Edxlsubplantype(pexprCorrelatedLOJ);

	if (EdxlSubPlanTypeScalar == edxlsubplantype)
	{
		// for correlated left outer join for scalar subplan type, we generate a scalar subplan
		BuildScalarSubplans(pdrgpcrInner, pexprInner, pdrgdxlcr, pdrgmdid, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);

		// now translate the scalar - references to the inner child will be
		// replaced by the subplan
		*ppdxlnScalar = PdxlnScalar(pexprScalar);

		return;
	}

	GPOS_ASSERT
		(
		EdxlSubPlanTypeAny == edxlsubplantype ||
		EdxlSubPlanTypeAll == edxlsubplantype ||
		EdxlSubPlanTypeExists == edxlsubplantype ||
		EdxlSubPlanTypeNotExists == edxlsubplantype
		);

	// for correlated left outer join with non-scalar subplan type,
	// we need to generate quantified/exitential subplan
	if (EdxlSubPlanTypeAny == edxlsubplantype || EdxlSubPlanTypeAll == edxlsubplantype)
	{
		(void) PdxlnQuantifiedSubplan(pdrgpcrInner, pexprCorrelatedLOJ, pdrgdxlcr, pdrgmdid, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
	}
	else
	{
		GPOS_ASSERT(EdxlSubPlanTypeExists == edxlsubplantype || EdxlSubPlanTypeNotExists == edxlsubplantype);
		(void) PdxlnExistentialSubplan(pdrgpcrInner, pexprCorrelatedLOJ, pdrgdxlcr, pdrgmdid, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
	}

	CExpression *pexprTrue = CUtils::PexprScalarConstBool(m_pmp, true /*fVal*/, false /*fNull*/);
	*ppdxlnScalar = PdxlnScalar(pexprTrue);
	pexprTrue->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::BuildSubplans
//
//	@doc:
//		Helper to build subplans of different types
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::BuildSubplans
	(
	CExpression *pexprCorrelatedNLJoin,
	DrgPdxlcr *pdrgdxlcr,
	DrgPmdid *pdrgmdid,
	CDXLNode **ppdxlnScalar, // output: scalar condition after replacing inner child reference with subplan
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(CUtils::FCorrelatedNLJoin(pexprCorrelatedNLJoin->Pop()));
	GPOS_ASSERT(NULL != ppdxlnScalar);

	CExpression *pexprInner = (*pexprCorrelatedNLJoin)[1];
	CExpression *pexprScalar = (*pexprCorrelatedNLJoin)[2];

	DrgPcr *pdrgpcrInner = CPhysicalNLJoin::PopConvert(pexprCorrelatedNLJoin->Pop())->PdrgPcrInner();
	GPOS_ASSERT(NULL != pdrgpcrInner);

	COperator::EOperatorId eopid = pexprCorrelatedNLJoin->Pop()->Eopid();
	CDXLNode *pdxlnSubPlan = NULL;
	switch (eopid)
	{
		case COperator::EopPhysicalCorrelatedLeftOuterNLJoin:
			BuildSubplansForCorrelatedLOJ(pexprCorrelatedNLJoin, pdrgdxlcr, pdrgmdid, ppdxlnScalar, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
			return;

		case COperator::EopPhysicalCorrelatedInnerNLJoin:
			BuildScalarSubplans(pdrgpcrInner, pexprInner, pdrgdxlcr, pdrgmdid, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);

			// now translate the scalar - references to the inner child will be
			// replaced by the subplan
			*ppdxlnScalar = PdxlnScalar(pexprScalar);
			return;

		case COperator::EopPhysicalCorrelatedInLeftSemiNLJoin:
		case COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin:
			pdxlnSubPlan = PdxlnQuantifiedSubplan(pdrgpcrInner, pexprCorrelatedNLJoin, pdrgdxlcr, pdrgmdid, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
			pdxlnSubPlan->AddRef();
			*ppdxlnScalar = pdxlnSubPlan;
			return;

		case COperator::EopPhysicalCorrelatedLeftSemiNLJoin:
		case COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin:
			pdxlnSubPlan = PdxlnExistentialSubplan(pdrgpcrInner, pexprCorrelatedNLJoin, pdrgdxlcr, pdrgmdid, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
			pdxlnSubPlan->AddRef();
			*ppdxlnScalar = pdxlnSubPlan;
			return;

		default:
				GPOS_ASSERT(!"Unsupported correlated join");
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnRestrictResult
//
//	@doc:
//		Helper to build a Result expression with project list
//		restricted to required column
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnRestrictResult
	(
	CDXLNode *pdxln,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pdxln);
	GPOS_ASSERT(NULL != pcr);

	CDXLNode *pdxlnProjListOld = (*pdxln)[0];
	const ULONG ulPrjElems = pdxlnProjListOld->UlArity();

	if (0 == ulPrjElems)
	{
		// failed to find project elements
		pdxln->Release();
		return NULL;
	}

	CDXLNode *pdxlnResult = pdxln;
	if (1 < ulPrjElems)
	{
		// restrict project list to required column
		CDXLScalarProjList *pdxlopPrL = GPOS_NEW(m_pmp) CDXLScalarProjList(m_pmp);
		CDXLNode *pdxlnProjListNew = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopPrL);

		for (ULONG ul = 0; ul < ulPrjElems; ul++)
		{
			CDXLNode *pdxlnChild = (*pdxlnProjListOld)[ul];
			CDXLScalarProjElem *pdxlPrjElem = CDXLScalarProjElem::PdxlopConvert(pdxlnChild->Pdxlop());
			if (pdxlPrjElem->UlId() == pcr->UlId())
			{
				// create a new project element that simply points to required column,
				// we cannot re-use pdxlnChild here since it may have a deep expression with columns inaccessible
				// above the child (inner) DXL expression
				CDXLNode *pdxlnPrEl = CTranslatorExprToDXLUtils::PdxlnProjElem(m_pmp, m_phmcrdxln, pcr);
				pdxlnProjListNew->AddChild(pdxlnPrEl);
			}
		}
		GPOS_ASSERT(1 == pdxlnProjListNew->UlArity());

		pdxlnResult = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLPhysicalResult(m_pmp));
		CDXLPhysicalProperties *pdxlprop = CTranslatorExprToDXLUtils::PdxlpropCopy(m_pmp, pdxln);
		pdxlnResult->SetProperties(pdxlprop);

		pdxlnResult->AddChild(pdxlnProjListNew);
		pdxlnResult->AddChild(PdxlnFilter(NULL));
		pdxlnResult->AddChild(GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp)));
		pdxlnResult->AddChild(pdxln);
	}

	return pdxlnResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnQuantifiedSubplan
//
//	@doc:
//		Helper to build subplans for quantified (ANY/ALL) subqueries
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnQuantifiedSubplan
	(
	DrgPcr *pdrgpcrInner,
	CExpression *pexprCorrelatedNLJoin,
	DrgPdxlcr *pdrgdxlcr,
	DrgPmdid *pdrgmdid,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	COperator *popCorrelatedJoin = pexprCorrelatedNLJoin->Pop();
	COperator::EOperatorId eopid = popCorrelatedJoin->Eopid();
	BOOL fCorrelatedLOJ = (COperator::EopPhysicalCorrelatedLeftOuterNLJoin == eopid);
	GPOS_ASSERT(COperator::EopPhysicalCorrelatedInLeftSemiNLJoin == eopid ||
			COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin == eopid ||
			fCorrelatedLOJ);

	EdxlSubPlanType edxlsubplantype = Edxlsubplantype(pexprCorrelatedNLJoin);
	GPOS_ASSERT_IMP(fCorrelatedLOJ, EdxlSubPlanTypeAny == edxlsubplantype || EdxlSubPlanTypeAll == edxlsubplantype);

	CExpression *pexprInner = (*pexprCorrelatedNLJoin)[1];
	CExpression *pexprScalar = (*pexprCorrelatedNLJoin)[2];

	// translate inner child
	CDXLNode *pdxlnInnerChild = Pdxln(pexprInner, NULL /*pdrgpcr*/, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	// find required column from inner child
	CColRef *pcrInner = (*pdrgpcrInner)[0];

	if (fCorrelatedLOJ)
	{
		// overwrite required inner column based on scalar expression

		CColRefSet *pcrsInner = CDrvdPropRelational::Pdprel(pexprInner->Pdp(CDrvdProp::EptRelational))->PcrsOutput();
		CColRefSet *pcrsUsed = GPOS_NEW(m_pmp) CColRefSet (m_pmp, *CDrvdPropScalar::Pdpscalar(pexprScalar->Pdp(CDrvdProp::EptScalar))->PcrsUsed());
		pcrsUsed->Intersection(pcrsInner);
		if (0 < pcrsUsed->CElements())
		{
			GPOS_ASSERT(1 == pcrsUsed->CElements());

			pcrInner = pcrsUsed->PcrFirst();
		}
		pcrsUsed->Release();
	}

	CDXLNode *pdxlnInner = PdxlnRestrictResult(pdxlnInnerChild, pcrInner);
	if (NULL == pdxlnInner)
	{
		GPOS_RAISE(gpopt::ExmaDXL, gpopt::ExmiExpr2DXLUnsupportedFeature, GPOS_WSZ_LIT("Outer references in the project list of a correlated subquery"));
	}

	// translate test expression
	CDXLNode *pdxlnTestExpr = PdxlnScalar(pexprScalar);

	const IMDTypeBool *pmdtypebool = m_pmda->PtMDType<IMDTypeBool>();
	IMDId *pmdid = pmdtypebool->Pmdid();
	pmdid->AddRef();

	// construct a subplan node, with the inner child under it
	CDXLNode *pdxlnSubPlan = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarSubPlan(m_pmp, pmdid, pdrgdxlcr, pdrgmdid, edxlsubplantype, pdxlnTestExpr));
	pdxlnSubPlan->AddChild(pdxlnInner);

	// add to hashmap
#ifdef GPOS_DEBUG
	BOOL fRes =
#endif // GPOS_DEBUG
		m_phmcrdxln->FInsert(const_cast<CColRef *>((*pdrgpcrInner)[0]), pdxlnSubPlan);
	GPOS_ASSERT(fRes);

	return pdxlnSubPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnProjectBoolConst
//
//	@doc:
//		Helper to add a project of bool constant on top of given DXL node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnProjectBoolConst
	(
	CDXLNode *pdxln,
	BOOL fVal
	)
{
	GPOS_ASSERT(NULL != pdxln);

	// create a new project element with bool value
	const IMDTypeBool *pmdtypebool = m_pmda->PtMDType<IMDTypeBool>();
	IMDId *pmdid = pmdtypebool->Pmdid();
	pmdid->AddRef();

	CDXLDatumBool *pdxldatum = GPOS_NEW(m_pmp) CDXLDatumBool(m_pmp, pmdid, false /* fNull */,  fVal);
	CDXLScalarConstValue *pdxlopConstValue = GPOS_NEW(m_pmp) CDXLScalarConstValue(m_pmp, pdxldatum);
	CColRef *pcr = m_pcf->PcrCreate(pmdtypebool);
	CDXLNode *pdxlnPrEl = PdxlnProjElem(pcr, GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopConstValue));

	CDXLScalarProjList *pdxlopPrL = GPOS_NEW(m_pmp) CDXLScalarProjList(m_pmp);
	CDXLNode *pdxlnProjList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopPrL);
	pdxlnProjList->AddChild(pdxlnPrEl);
	CDXLNode *pdxlnResult = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLPhysicalResult(m_pmp));
	CDXLPhysicalProperties *pdxlprop = CTranslatorExprToDXLUtils::PdxlpropCopy(m_pmp, pdxln);
	pdxlnResult->SetProperties(pdxlprop);

	pdxlnResult->AddChild(pdxlnProjList);
	pdxlnResult->AddChild(PdxlnFilter(NULL));
	pdxlnResult->AddChild(GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp)));
	pdxlnResult->AddChild(pdxln);

	return pdxlnResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::EdxlsubplantypeCorrelatedLOJ
//
//	@doc:
//		Helper to find subplan type from a correlated left outer
//		join expression
//
//---------------------------------------------------------------------------
EdxlSubPlanType
CTranslatorExprToDXL::EdxlsubplantypeCorrelatedLOJ
	(
	CExpression *pexprCorrelatedLOJ
	)
{
	GPOS_ASSERT(NULL != pexprCorrelatedLOJ);
	GPOS_ASSERT(COperator::EopPhysicalCorrelatedLeftOuterNLJoin == pexprCorrelatedLOJ->Pop()->Eopid());

	COperator::EOperatorId eopidSubq =
			CPhysicalCorrelatedLeftOuterNLJoin::PopConvert(pexprCorrelatedLOJ->Pop())->EopidOriginSubq();
	switch (eopidSubq)
	{
		case COperator::EopScalarSubquery:
			return EdxlSubPlanTypeScalar;

		case COperator::EopScalarSubqueryAll:
			return EdxlSubPlanTypeAll;

		case COperator::EopScalarSubqueryAny:
			return EdxlSubPlanTypeAny;

		case COperator::EopScalarSubqueryExists:
			return EdxlSubPlanTypeExists;

		case COperator::EopScalarSubqueryNotExists:
			return  EdxlSubPlanTypeNotExists;

		default:
			GPOS_ASSERT(!"Unexpected origin subquery in correlated left outer join");
	}

	return EdxlSubPlanTypeSentinel;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Edxlsubplantype
//
//	@doc:
//		Helper to find subplan type from a correlated join expression
//
//---------------------------------------------------------------------------
EdxlSubPlanType
CTranslatorExprToDXL::Edxlsubplantype
	(
	CExpression *pexprCorrelatedNLJoin
	)
{
	GPOS_ASSERT(NULL != pexprCorrelatedNLJoin);
	GPOS_ASSERT(CUtils::FCorrelatedNLJoin(pexprCorrelatedNLJoin->Pop()));

	COperator::EOperatorId eopid = pexprCorrelatedNLJoin->Pop()->Eopid();
	switch (eopid)
	{
		case COperator::EopPhysicalCorrelatedLeftOuterNLJoin:
			return EdxlsubplantypeCorrelatedLOJ(pexprCorrelatedNLJoin);

		case COperator::EopPhysicalCorrelatedInnerNLJoin:
			return EdxlSubPlanTypeScalar;

		case COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin:
			return EdxlSubPlanTypeAll;

		case COperator::EopPhysicalCorrelatedInLeftSemiNLJoin:
			return EdxlSubPlanTypeAny;

		case COperator::EopPhysicalCorrelatedLeftSemiNLJoin:
			return EdxlSubPlanTypeExists;

		case COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin:
			return EdxlSubPlanTypeNotExists;

		default:
			GPOS_ASSERT(!"Unexpected correlated join");
	}

	return EdxlSubPlanTypeSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnExistentialSubplan
//
//	@doc:
//		Helper to build subplans for existential subqueries
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnExistentialSubplan
	(
	DrgPcr *pdrgpcrInner,
	CExpression *pexprCorrelatedNLJoin,
	DrgPdxlcr *pdrgdxlcr,
	DrgPmdid *pdrgmdid,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
#ifdef GPOS_DEBUG
	COperator::EOperatorId eopid = pexprCorrelatedNLJoin->Pop()->Eopid();
	BOOL fCorrelatedLOJ = (COperator::EopPhysicalCorrelatedLeftOuterNLJoin == eopid);
#endif // GPOS_DEBUG
	GPOS_ASSERT(COperator::EopPhysicalCorrelatedLeftSemiNLJoin == eopid ||
			COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin == eopid ||
			fCorrelatedLOJ);

	EdxlSubPlanType edxlsubplantype = Edxlsubplantype(pexprCorrelatedNLJoin);
	GPOS_ASSERT_IMP(fCorrelatedLOJ, EdxlSubPlanTypeExists == edxlsubplantype || EdxlSubPlanTypeNotExists == edxlsubplantype);

	// translate inner child
	CExpression *pexprInner = (*pexprCorrelatedNLJoin)[1];
	
	CDXLNode *pdxlnInnerChild = Pdxln(pexprInner, NULL /*pdrgpcr*/, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);
	CDXLNode *pdxlnInnerProjList = (*pdxlnInnerChild)[0];
	CDXLNode *pdxlnInner = NULL;
	if (0 == pdxlnInnerProjList->UlArity())
	{
		// no requested columns from subplan, add a dummy boolean constant to project list
		pdxlnInner = PdxlnProjectBoolConst(pdxlnInnerChild, true /*fVal*/);
	}
	else
	{
		// restrict requested columns to required inner column
		pdxlnInner = PdxlnRestrictResult(pdxlnInnerChild, (*pdrgpcrInner)[0]);
	}
	
	if (NULL == pdxlnInner)
	{
		GPOS_RAISE(gpopt::ExmaDXL, gpopt::ExmiExpr2DXLUnsupportedFeature, GPOS_WSZ_LIT("Outer references in the project list of a correlated subquery"));
	}

	const IMDTypeBool *pmdtypebool = m_pmda->PtMDType<IMDTypeBool>();
	IMDId *pmdid = pmdtypebool->Pmdid();
	pmdid->AddRef();

	// construct a subplan node, with the inner child under it
	CDXLNode *pdxlnSubPlan =
		GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarSubPlan(m_pmp, pmdid, pdrgdxlcr, pdrgmdid, edxlsubplantype, NULL /*pdxlnTestExpr*/));
	pdxlnSubPlan->AddChild(pdxlnInner);

	// add to hashmap
#ifdef GPOS_DEBUG
	BOOL fRes =
#endif // GPOS_DEBUG
		m_phmcrdxln->FInsert(const_cast<CColRef *>((*pdrgpcrInner)[0]), pdxlnSubPlan);
	GPOS_ASSERT(fRes);

	return pdxlnSubPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::BuildScalarSubplans
//
//	@doc:
//		Helper to build subplans from inner column references and store
//		generated subplans in subplan map
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::BuildScalarSubplans
	(
	DrgPcr *pdrgpcrInner,
	CExpression *pexprInner,
	DrgPdxlcr *pdrgdxlcr,
	DrgPmdid *pdrgmdid,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	const ULONG ulSize = pdrgpcrInner->UlLength();

	DrgPdxln *pdrgpdxlnInner = GPOS_NEW(m_pmp) DrgPdxln(m_pmp);
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		// for each subplan, we need to re-translate inner expression
		CDXLNode *pdxlnInnerChild = Pdxln(pexprInner, NULL /*pdrgpcr*/, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);
		CDXLNode *pdxlnInner = PdxlnRestrictResult(pdxlnInnerChild, (*pdrgpcrInner)[ul]);
		if (NULL == pdxlnInner)
		{
			GPOS_RAISE(gpopt::ExmaDXL, gpopt::ExmiExpr2DXLUnsupportedFeature, GPOS_WSZ_LIT("Outer references in the project list of a correlated subquery"));
		}
		pdrgpdxlnInner->Append(pdxlnInner);
	}

	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CDXLNode *pdxlnInner = (*pdrgpdxlnInner)[ul];
		pdxlnInner->AddRef();
		if (0 < ul)
		{
			// if there is more than one subplan, we need to add-ref passed arrays
			pdrgdxlcr->AddRef();
			pdrgmdid->AddRef();
		}
		const CColRef *pcrInner = (*pdrgpcrInner)[ul];
		BuildDxlnSubPlan(pdxlnInner, pcrInner, pdrgdxlcr, pdrgmdid);
	}

	pdrgpdxlnInner->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PcrsOuterRefsForCorrelatedNLJoin
//
//	@doc:
//		Return outer refs in correlated join inner child
//
//---------------------------------------------------------------------------
CColRefSet *
CTranslatorExprToDXL::PcrsOuterRefsForCorrelatedNLJoin
	(
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(CUtils::FCorrelatedNLJoin(pexpr->Pop()));

	CExpression *pexprInnerChild = (*pexpr)[1];

	// get inner child's relational properties
	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexprInnerChild->Pdp(CDrvdProp::EptRelational));

	return pdprel->PcrsOuter();
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnCorrelatedNLJoin
//
//	@doc:
//		Translate correlated NLJ expression.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnCorrelatedNLJoin
	(
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(CUtils::FCorrelatedNLJoin(pexpr->Pop()));

	// extract components
	CExpression *pexprOuterChild = (*pexpr)[0];
	CExpression *pexprInnerChild = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	// outer references in the inner child
	DrgPdxlcr *pdrgdxlcr = GPOS_NEW(m_pmp) DrgPdxlcr(m_pmp);
	DrgPmdid *pdrgmdid = GPOS_NEW(m_pmp) DrgPmdid(m_pmp);

	CColRefSet *pcrsOuter = PcrsOuterRefsForCorrelatedNLJoin(pexpr);
	CColRefSetIter crsi(*pcrsOuter);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		CMDName *pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pcr->Name().Pstr());
		CDXLColRef *pdxlcr = GPOS_NEW(m_pmp) CDXLColRef(m_pmp, pmdname, pcr->UlId());
		IMDId *pmdid = pcr->Pmdtype()->Pmdid();
		pmdid->AddRef();
		pdrgdxlcr->Append(pdxlcr);
		pdrgmdid->Append(pmdid);
	}

	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();
	CDXLNode *pdxlnCond = NULL;
	if (CUtils::FScalarConstTrue(pexprScalar) &&
		COperator::EopPhysicalCorrelatedInnerNLJoin == eopid)
	{
		// translate relational inner child expression
		CDXLNode *pdxlnInnerChild = Pdxln
									(
									pexprInnerChild, 
									NULL, // pdrgpcr, 
									pdrgpdsBaseTables, 
									pulNonGatherMotions, 
									pfDML, 
									false, // fRemap
									false // fRoot
									);

		// if the filter predicate is a constant TRUE, create a subplan that returns
		// Boolean from the inner child, and use that as the scalar condition
		pdxlnCond = PdxlnBooleanScalarWithSubPlan(pdxlnInnerChild, pdrgdxlcr, pdrgmdid);
	}
	else
	{
		BuildSubplans(pexpr, pdrgdxlcr, pdrgmdid, &pdxlnCond, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
	}

	// extract dxl properties from correlated join
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexpr);
	CDXLNode *pdxln = NULL;

	switch (pexprOuterChild->Pop()->Eopid())
	{
		case COperator::EopPhysicalTableScan:
		{
			// create and return a table scan node
			pdxln = PdxlnTblScanFromNLJoinOuter(pexprOuterChild, pdxlnCond, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pdxlprop);
			break;
		}

		case COperator::EopPhysicalFilter:
		{
			// get the original condition from the filter node
			CExpression *pexprOrigCond = (*pexprOuterChild)[1];
			CDXLNode *pdxlnOrigCond = PdxlnScalar(pexprOrigCond);
			// create a new AND expression
			CDXLNode *pdxlnBoolExpr = PdxlnScBoolExpr
											(
											Edxland,
											pdxlnOrigCond,
											pdxlnCond
											);

			pdxln = PdxlnResultFromNLJoinOuter(pexprOuterChild, pdxlnBoolExpr, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, pdxlprop);
			break;
		}

		default:
		{
			// for a true condition, just translate the child
			if (CTranslatorExprToDXLUtils::FScalarConstTrue(m_pmda, pdxlnCond))
			{
				pdxlnCond->Release();
				pdxln = Pdxln(pexprOuterChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);
			}
			else
			{
				// create a result node over outer child
				pdxlprop->AddRef();
				pdxln = PdxlnResult(pexprOuterChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, pdxlnCond, pdxlprop);
			}
		}
	}

	pdxlprop->Release();
	return pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::BuildDxlnSubPlan
//
//	@doc:
//		Construct a scalar dxl node with a subplan as its child. Also put this
//		subplan in the hashmap with its output column, so that anyone who
//		references that column can use the subplan
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::BuildDxlnSubPlan
	(
	CDXLNode *pdxlnRelChild,
	const CColRef *pcr,
	DrgPdxlcr *pdrgdxlcr,
	DrgPmdid *pdrgmdid
	)
{
	GPOS_ASSERT(NULL != pcr);
	IMDId *pmdid = pcr->Pmdtype()->Pmdid();
	pmdid->AddRef();

	// construct a subplan node, with the inner child under it
	CDXLNode *pdxlnSubPlan = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarSubPlan(m_pmp, pmdid, pdrgdxlcr, pdrgmdid, EdxlSubPlanTypeScalar, NULL));
	pdxlnSubPlan->AddChild(pdxlnRelChild);

	// add to hashmap
#ifdef GPOS_DEBUG
	BOOL fRes =
#endif // GPOS_DEBUG
	m_phmcrdxln->FInsert(const_cast<CColRef *>(pcr), pdxlnSubPlan);
	GPOS_ASSERT(fRes);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnBooleanScalarWithSubPlan
//
//	@doc:
//		Construct a boolean scalar dxl node with a subplan as its child. The
//		sublan has a boolean output column, and has	the given relational child
//		under it
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnBooleanScalarWithSubPlan
	(
	CDXLNode *pdxlnRelChild,
	DrgPdxlcr *pdrgdxlcr,
	DrgPmdid *pdrgmdid
	)
{
	// create a new project element (const:true), and replace the first child with it
	const IMDTypeBool *pmdtypebool = m_pmda->PtMDType<IMDTypeBool>();
	IMDId *pmdid = pmdtypebool->Pmdid();
	pmdid->AddRef();

	CDXLDatumBool *pdxldatum = GPOS_NEW(m_pmp) CDXLDatumBool(m_pmp, pmdid, false /* fNull */, true /* fVal */);
	CDXLScalarConstValue *pdxlopConstValue = GPOS_NEW(m_pmp) CDXLScalarConstValue(m_pmp, pdxldatum);

	CColRef *pcr = m_pcf->PcrCreate(pmdtypebool);

	CDXLNode *pdxlnPrEl = PdxlnProjElem(pcr, GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopConstValue));

	// create a new Result node for the created project element
	CDXLNode *pdxlnProjListNew = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarProjList(m_pmp));
	pdxlnProjListNew->AddChild(pdxlnPrEl);
	CDXLNode *pdxlnResult = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLPhysicalResult(m_pmp));
	CDXLPhysicalProperties *pdxlprop = CTranslatorExprToDXLUtils::PdxlpropCopy(m_pmp, pdxlnRelChild);
	pdxlnResult->SetProperties(pdxlprop);
	pdxlnResult->AddChild(pdxlnProjListNew);
	pdxlnResult->AddChild(PdxlnFilter(NULL));
	pdxlnResult->AddChild(GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOneTimeFilter(m_pmp)));
	pdxlnResult->AddChild(pdxlnRelChild);

	// construct a subplan node, with the Result node under it
	pmdid->AddRef();
	CDXLNode *pdxlnSubPlan = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarSubPlan(m_pmp, pmdid, pdrgdxlcr, pdrgmdid, EdxlSubPlanTypeScalar, NULL));
	pdxlnSubPlan->AddChild(pdxlnResult);

	return pdxlnSubPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScBoolExpr
//
//	@doc:
//		Create a DXL scalar boolean node given two DXL boolean nodes
//		and a boolean op
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScBoolExpr
	(
	EdxlBoolExprType boolexptype,
	CDXLNode *pdxlnLeft,
	CDXLNode *pdxlnRight
	)
{
	CDXLNode *pdxlnBoolExpr = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarBoolExpr(m_pmp, boolexptype));

	pdxlnBoolExpr->AddChild(pdxlnLeft);
	pdxlnBoolExpr->AddChild(pdxlnRight);

	return pdxlnBoolExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnTblScanFromNLJoinOuter
//
//	@doc:
//		Create a DXL table scan node from the outer child of a NLJ
//		and a DXL scalar condition. Used for translated correlated
//		subqueries.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnTblScanFromNLJoinOuter
	(
	CExpression *pexprRelational,
	CDXLNode *pdxlnCond,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *, // pulNonGatherMotions,
	CDXLPhysicalProperties *pdxlprop
	)
{
	// create a table scan over the input expression, without a filter
	pdxlprop->AddRef();
	CDXLNode *pdxlnTblScan = PdxlnTblScan
								(
								pexprRelational,
								NULL, //pcrsOutput
								pdrgpcr,
								pdrgpdsBaseTables, 
								NULL, //pexprScalar
								pdxlprop
								);

	if (!CTranslatorExprToDXLUtils::FScalarConstTrue(m_pmda, pdxlnCond))
	{
		// add the new filter to the table scan replacing its original
		// empty filter
		CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnCond);
		pdxlnTblScan->ReplaceChild(EdxltsIndexFilter /*ulPos*/, pdxlnFilter);
	}
	else
	{
		// not used
		pdxlnCond->Release();
	}

	return pdxlnTblScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnResultFromNLJoinOuter
//
//	@doc:
//		Create a DXL result node from the outer child of a NLJ
//		and a DXL scalar condition. Used for translated correlated
//		subqueries.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnResultFromNLJoinOuter
	(
	CExpression *pexprRelational,
	CDXLNode *pdxlnCond,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	CDXLPhysicalProperties *pdxlprop
	)
{
	// create a result node from the input expression
	CDXLNode *pdxlnResult = PdxlnResult(pexprRelational, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, pdxlprop);

	// add the new filter to the result replacing its original
	// empty filter
	CDXLNode *pdxlnFilter = PdxlnFilter(pdxlnCond);
	pdxlnResult->ReplaceChild(EdxltsIndexFilter /*ulPos*/, pdxlnFilter);

	return pdxlnResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::StoreIndexNLJOuterRefs
//
//	@doc:
//		Store outer references in index NLJ inner child into global map
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::StoreIndexNLJOuterRefs
	(
	CPhysical *pop
	)
{
	DrgPcr *pdrgpcr = CPhysicalInnerIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs();
	const ULONG ulSize = pdrgpcr->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CColRef *pcr = (*pdrgpcr)[ul];
		if (NULL == m_phmcrdxlnIndexLookup->PtLookup(pcr))
		{
			CDXLNode *pdxln = CTranslatorExprToDXLUtils::PdxlnIdent(m_pmp, m_phmcrdxln, m_phmcrdxlnIndexLookup, pcr);
#ifdef 	GPOS_DEBUG
			BOOL fInserted =
#endif // GPOS_DEBUG
			m_phmcrdxlnIndexLookup->FInsert(pcr, pdxln);
			GPOS_ASSERT(fInserted);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnNLJoin
//
//	@doc:
//		Create a DXL nested loop join node from an optimizer nested loop
//		join expression
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnNLJoin
	(
	CExpression *pexprInnerNLJ,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprInnerNLJ);
	GPOS_ASSERT(3 == pexprInnerNLJ->UlArity());

	// extract components
	CPhysical *pop = CPhysical::PopConvert(pexprInnerNLJ->Pop());
	
	CExpression *pexprOuterChild = (*pexprInnerNLJ)[0];
	CExpression *pexprInnerChild = (*pexprInnerNLJ)[1];
	CExpression *pexprScalar = (*pexprInnerNLJ)[2];


#ifdef GPOS_DEBUG
	// get children's relational properties
	CDrvdPropRelational *pdprelOuter =
			CDrvdPropRelational::Pdprel(pexprOuterChild->Pdp(CDrvdProp::EptRelational));

	CDrvdPropRelational *pdprelInner =
			CDrvdPropRelational::Pdprel(pexprInnerChild->Pdp(CDrvdProp::EptRelational));

	GPOS_ASSERT_IMP(COperator::EopPhysicalInnerIndexNLJoin != pop->Eopid(), pdprelInner->PcrsOuter()->FDisjoint(pdprelOuter->PcrsOutput()) &&
			"detected outer references in NL inner child");
#endif // GPOS_DEBUG

	EdxlJoinType edxljt = EdxljtSentinel;
	BOOL fIndexNLJ = false;
	switch (pop->Eopid())
	{
		case COperator::EopPhysicalInnerNLJoin:
			edxljt = EdxljtInner;
			break;

		case COperator::EopPhysicalInnerIndexNLJoin:
			edxljt = EdxljtInner;
			fIndexNLJ = true;
			StoreIndexNLJOuterRefs(pop);
			break;
	
		case COperator::EopPhysicalLeftOuterNLJoin:
			edxljt = EdxljtLeft;
			break;
		
		case COperator::EopPhysicalLeftSemiNLJoin:
			edxljt = EdxljtIn;
			break;
					
		case COperator::EopPhysicalLeftAntiSemiNLJoin:
			edxljt = EdxljtLeftAntiSemijoin;
			break;
		
		case COperator::EopPhysicalLeftAntiSemiNLJoinNotIn:
			edxljt = EdxljtLeftAntiSemijoinNotIn;
			break;

		default:
			GPOS_ASSERT(!"Invalid join type");
	}

	// translate relational child expressions
	CDXLNode *pdxlnOuterChild = Pdxln(pexprOuterChild, NULL /*pdrgpcr*/, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);
	CDXLNode *pdxlnInnerChild = Pdxln(pexprInnerChild, NULL /*pdrgpcr*/, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);
	CDXLNode *pdxlnCond = PdxlnScalar(pexprScalar);

	CDXLNode *pdxlnJoinFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarJoinFilter(m_pmp));
	if (NULL != pdxlnCond)
	{
		pdxlnJoinFilter->AddChild(pdxlnCond);
	}

	// construct a join node
	CDXLPhysicalNLJoin *pdxlopNLJ = GPOS_NEW(m_pmp) CDXLPhysicalNLJoin(m_pmp, edxljt,fIndexNLJ);

	// construct projection list
	// compute required columns
	GPOS_ASSERT(NULL != pexprInnerNLJ->Prpp());
	CColRefSet *pcrsOutput = pexprInnerNLJ->Prpp()->PcrsRequired();

	CDXLNode *pdxlnProjList = PdxlnProjList(pcrsOutput, pdrgpcr);	

	CDXLNode *pdxlnNLJ = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopNLJ);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprInnerNLJ);
	pdxlnNLJ->SetProperties(pdxlprop);
	
	// construct an empty plan filter
	CDXLNode *pdxlnFilter = PdxlnFilter(NULL);

	// add children
	pdxlnNLJ->AddChild(pdxlnProjList);
	pdxlnNLJ->AddChild(pdxlnFilter);
	pdxlnNLJ->AddChild(pdxlnJoinFilter);
	pdxlnNLJ->AddChild(pdxlnOuterChild);
	pdxlnNLJ->AddChild(pdxlnInnerChild);

#ifdef GPOS_DEBUG
	pdxlopNLJ->AssertValid(pdxlnNLJ, false /* fValidateChildren */);
#endif

	return pdxlnNLJ;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::EdxljtHashJoin
//
//	@doc:
//		Return hash join type
//---------------------------------------------------------------------------
EdxlJoinType
CTranslatorExprToDXL::EdxljtHashJoin
	(
	CPhysicalHashJoin *popHJ
	)
{
	GPOS_ASSERT(CUtils::FHashJoin(popHJ));

	switch (popHJ->Eopid())
	{
		case COperator::EopPhysicalInnerHashJoin:
			return EdxljtInner;

		case COperator::EopPhysicalLeftOuterHashJoin:
			return EdxljtLeft;

		case COperator::EopPhysicalLeftSemiHashJoin:
			return EdxljtIn;

		case COperator::EopPhysicalLeftAntiSemiHashJoin:
			return EdxljtLeftAntiSemijoin;

		case COperator::EopPhysicalLeftAntiSemiHashJoinNotIn:
			return EdxljtLeftAntiSemijoinNotIn;

		default:
			GPOS_ASSERT(!"Invalid join type");
			return EdxljtSentinel;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnHashJoin
//
//	@doc:
//		Create a DXL hash join node from an optimizer hash join expression.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnHashJoin
	(
	CExpression *pexprHJ,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprHJ);

	GPOS_ASSERT(3 == pexprHJ->UlArity());

	// extract components
	CPhysicalHashJoin *popHJ = CPhysicalHashJoin::PopConvert(pexprHJ->Pop());
	CExpression *pexprOuterChild = (*pexprHJ)[0];
	CExpression *pexprInnerChild = (*pexprHJ)[1];
	CExpression *pexprScalar = (*pexprHJ)[2];

	EdxlJoinType edxljt = EdxljtHashJoin(popHJ);
	GPOS_ASSERT(popHJ->PdrgpexprOuterKeys()->UlLength() == popHJ->PdrgpexprInnerKeys()->UlLength());

	// translate relational child expression
	CDXLNode *pdxlnOuterChild = Pdxln(pexprOuterChild, NULL /*pdrgpcr*/, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);
	CDXLNode *pdxlnInnerChild = Pdxln(pexprInnerChild, NULL /*pdrgpcr*/, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	// construct hash condition
	CDXLNode *pdxlnHashCondList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarHashCondList(m_pmp));

	// output of outer side
	CColRefSet *pcrsOuter =  CDrvdPropRelational::Pdprel(pexprOuterChild->Pdp(CDrvdProp::EptRelational))->PcrsOutput();

#ifdef GPOS_DEBUG
	// output of inner side
	CColRefSet *pcrsInner = CDrvdPropRelational::Pdprel(pexprInnerChild->Pdp(CDrvdProp::EptRelational))->PcrsOutput();
	ULONG ulHashJoinPreds = 0;
#endif // GPOS_DEBUG

	DrgPexpr *pdrgpexprPredicates = CPredicateUtils::PdrgpexprConjuncts(m_pmp, pexprScalar);
	DrgPexpr *pdrgpexprRemainingPredicates = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);
	const ULONG ulSize = pdrgpexprPredicates->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CExpression *pexprPred = (*pdrgpexprPredicates)[ul];
		if (CPhysicalJoin::FHashJoinCompatible(pexprPred, pexprOuterChild, pexprInnerChild))
		{
			 CExpression *pexprPredOuter = NULL;
			 CExpression *pexprPredInner = NULL;
			 CPhysicalJoin::ExtractHashJoinExpressions(pexprPred, &pexprPredOuter, &pexprPredInner);

			 // align extracted columns with outer and inner children of the join
			 CColRefSet *pcrsPredInner = CDrvdPropScalar::Pdpscalar(pexprPredInner->PdpDerive())->PcrsUsed();
#ifdef GPOS_DEBUG
			 CColRefSet *pcrsPredOuter = CDrvdPropScalar::Pdpscalar(pexprPredOuter->PdpDerive())->PcrsUsed();
#endif // GPOS_DEBUG
			 if (pcrsOuter->FSubset(pcrsPredInner))
			 {
				 // extracted expressions are not aligned with join children, we need to swap them
				 GPOS_ASSERT(pcrsInner->FSubset(pcrsPredOuter));
				 std::swap(pexprPredOuter, pexprPredInner);
#ifdef GPOS_DEBUG
				 std::swap(pcrsPredOuter, pcrsPredInner);
#endif
			 }
			 GPOS_ASSERT(pcrsOuter->FSubset(pcrsPredOuter) && pcrsInner->FSubset(pcrsPredInner) &&
					 "hash join keys are not aligned with hash join children");

			 pexprPredOuter->AddRef();
			 pexprPredInner->AddRef();
			 // create hash join predicate based on conjunct type
			 if (CPredicateUtils::FEquality(pexprPred))
			 {
				pexprPred = CUtils::PexprScalarEqCmp(m_pmp, pexprPredOuter, pexprPredInner);
			 }
			 else
			 {
				GPOS_ASSERT(CPredicateUtils::FINDF(pexprPred));
				pexprPred = CUtils::PexprINDF(m_pmp, pexprPredOuter, pexprPredInner);
			 }

			 CDXLNode *pdxlnPred = PdxlnScalar(pexprPred);
			 pdxlnHashCondList->AddChild(pdxlnPred);
			 pexprPred->Release();
#ifdef GPOS_DEBUG
			 ulHashJoinPreds ++;
#endif // GPOS_DEBUG
		}
		else
		{
			pexprPred->AddRef();
			pdrgpexprRemainingPredicates->Append(pexprPred);
		}
	}
	GPOS_ASSERT(popHJ->PdrgpexprOuterKeys()->UlLength() == ulHashJoinPreds);

	CDXLNode *pdxlnJoinFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarJoinFilter(m_pmp));
	if (0 < pdrgpexprRemainingPredicates->UlLength())
	{
		CExpression *pexprJoinCond = CPredicateUtils::PexprConjunction(m_pmp, pdrgpexprRemainingPredicates);
		CDXLNode *pdxlnJoinCond = PdxlnScalar(pexprJoinCond);
		pdxlnJoinFilter->AddChild(pdxlnJoinCond);
		pexprJoinCond->Release();
	}
	else
	{
		pdrgpexprRemainingPredicates->Release();
	}

	// construct a hash join node
	CDXLPhysicalHashJoin *pdxlopHJ = GPOS_NEW(m_pmp) CDXLPhysicalHashJoin(m_pmp, edxljt);

	// construct projection list from required columns
	GPOS_ASSERT(NULL != pexprHJ->Prpp());
	CColRefSet *pcrsOutput = pexprHJ->Prpp()->PcrsRequired();
	CDXLNode *pdxlnProjList = PdxlnProjList(pcrsOutput, pdrgpcr);
	
	CDXLNode *pdxlnHJ = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopHJ);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprHJ);
	pdxlnHJ->SetProperties(pdxlprop);

	// construct an empty plan filter
	CDXLNode *pdxlnFilter = PdxlnFilter(NULL);

	// add children
	pdxlnHJ->AddChild(pdxlnProjList);
	pdxlnHJ->AddChild(pdxlnFilter);
	pdxlnHJ->AddChild(pdxlnJoinFilter);
	pdxlnHJ->AddChild(pdxlnHashCondList);
	pdxlnHJ->AddChild(pdxlnOuterChild);
	pdxlnHJ->AddChild(pdxlnInnerChild);
	
	// cleanup 
	pdrgpexprPredicates->Release();

#ifdef GPOS_DEBUG
	pdxlopHJ->AssertValid(pdxlnHJ, false /* fValidateChildren */);
#endif

	return pdxlnHJ;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::CheckValidity
//
//	@doc:
//		Check if the motion node is valid
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::CheckValidity
	(
	CDXLPhysicalMotion *pdxlopMotion
	)

{
	// validate the input segment info for Gather Motion
	// if Gather has only 1 segment when there are more hosts
	// it's obviously invalid and we fall back
	if (EdxlopPhysicalMotionGather == pdxlopMotion->Edxlop())
	{
		if((pdxlopMotion->PdrgpiInputSegIds()->UlLength() == 1) && COptCtxt::PoctxtFromTLS()->Pcm()->UlHosts() > 1)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiExpr2DXLUnsupportedFeature, GPOS_WSZ_LIT("GatherMotion has 1 input but there are more segments in the system"));
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnMotion
//
//	@doc:
//		Create a DXL motion node from an optimizer motion expression
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnMotion
	(
	CExpression *pexprMotion,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprMotion);
	GPOS_ASSERT(1 == pexprMotion->UlArity());

	// extract components
	CExpression *pexprChild = (*pexprMotion)[0];

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, true /*fRemap*/, false /*fRoot*/);

	// construct a motion node
	CDXLPhysicalMotion *pdxlopMotion = NULL;
	BOOL fDuplicateHazardMotion = CUtils::FDuplicateHazardMotion(pexprMotion);
	switch (pexprMotion->Pop()->Eopid())
	{
		case COperator::EopPhysicalMotionGather:
			pdxlopMotion = GPOS_NEW(m_pmp) CDXLPhysicalGatherMotion(m_pmp);
			break;
		
		case COperator::EopPhysicalMotionBroadcast:
			pdxlopMotion = GPOS_NEW(m_pmp) CDXLPhysicalBroadcastMotion(m_pmp);
			break;
		
		case COperator::EopPhysicalMotionHashDistribute:
			pdxlopMotion = GPOS_NEW(m_pmp) CDXLPhysicalRedistributeMotion(m_pmp, fDuplicateHazardMotion);
			break;
			
		case COperator::EopPhysicalMotionRandom:
			pdxlopMotion = GPOS_NEW(m_pmp) CDXLPhysicalRandomMotion(m_pmp, fDuplicateHazardMotion);
			break;
		
		case COperator::EopPhysicalMotionRoutedDistribute:
			{
				CPhysicalMotionRoutedDistribute *popMotion = 
						CPhysicalMotionRoutedDistribute::PopConvert(pexprMotion->Pop()); 
				CColRef *pcrSegmentId = dynamic_cast<const CDistributionSpecRouted* >(popMotion->Pds())->Pcr();
				
				pdxlopMotion = GPOS_NEW(m_pmp) CDXLPhysicalRoutedDistributeMotion(m_pmp, pcrSegmentId->UlId());
				break;
			}
		default:
			GPOS_ASSERT(!"Unrecognized motion type");
	}
	
	if (COperator::EopPhysicalMotionGather != pexprMotion->Pop()->Eopid())
	{
		(*pulNonGatherMotions)++;
	}
	
	GPOS_ASSERT(NULL != pdxlopMotion);
	
	// construct project list from child project list
	GPOS_ASSERT(NULL != pdxlnChild && 1 <= pdxlnChild->UlArity());
	CDXLNode *pdxlnProjListChild = (*pdxlnChild)[0];
	
	CDXLNode *pdxlnProjList = CTranslatorExprToDXLUtils::PdxlnProjListFromChildProjList(m_pmp, m_pcf, m_phmcrdxln, pdxlnProjListChild);
	
	// set input and output segment information
	pdxlopMotion->SetSegmentInfo(PdrgpiInputSegIds(pexprMotion), PdrgpiOutputSegIds(pexprMotion));

	CheckValidity(pdxlopMotion);

	CDXLNode *pdxlnMotion = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopMotion);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprMotion);
	pdxlnMotion->SetProperties(pdxlprop);

	// construct an empty filter node
	CDXLNode *pdxlnFilter = PdxlnFilter(NULL /*pdxlnCond*/);

	// construct sort column list
	CDXLNode *pdxlnSortColList = PdxlnSortColList(pexprMotion);

	// add children
	pdxlnMotion->AddChild(pdxlnProjList);
	pdxlnMotion->AddChild(pdxlnFilter);
	pdxlnMotion->AddChild(pdxlnSortColList);
	
	if (COperator::EopPhysicalMotionHashDistribute == pexprMotion->Pop()->Eopid())
	{
		// construct a hash expr list node
		CPhysicalMotionHashDistribute *popHashDistribute = CPhysicalMotionHashDistribute::PopConvert(pexprMotion->Pop());
		CDistributionSpecHashed *pdsHashed = CDistributionSpecHashed::PdsConvert(popHashDistribute->Pds());
		CDXLNode *pdxlnHashExprList = PdxlnHashExprList(pdsHashed->Pdrgpexpr());
		pdxlnMotion->AddChild(pdxlnHashExprList);
	}
	
	pdxlnMotion->AddChild(pdxlnChild);

#ifdef GPOS_DEBUG
	pdxlopMotion->AssertValid(pdxlnMotion, false /* fValidateChildren */);
#endif

	return pdxlnMotion;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnMaterialize
//
//	@doc:
//		Create a DXL materialize node from an optimizer spool expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnMaterialize
	(
	CExpression *pexprSpool,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprSpool);

	GPOS_ASSERT(1 == pexprSpool->UlArity());

	// extract components
	CExpression *pexprChild = (*pexprSpool)[0];

	// translate relational child expression
	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	// construct a materialize node
	CDXLPhysicalMaterialize *pdxlopMat = GPOS_NEW(m_pmp) CDXLPhysicalMaterialize(m_pmp, true /* fEager */);

	// construct project list from child project list
	GPOS_ASSERT(NULL != pdxlnChild && 1 <= pdxlnChild->UlArity());
	CDXLNode *pdxlnProjListChild = (*pdxlnChild)[0];
	CDXLNode *pdxlnProjList = CTranslatorExprToDXLUtils::PdxlnProjListFromChildProjList(m_pmp, m_pcf, m_phmcrdxln, pdxlnProjListChild);
	
	CDXLNode *pdxlnMaterialize = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopMat);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprSpool);
	pdxlnMaterialize->SetProperties(pdxlprop);
	
	// construct an empty filter node
	CDXLNode *pdxlnFilter = PdxlnFilter(NULL /* pdxlnCond */);
	
	// add children
	pdxlnMaterialize->AddChild(pdxlnProjList);
	pdxlnMaterialize->AddChild(pdxlnFilter);
	pdxlnMaterialize->AddChild(pdxlnChild);

#ifdef GPOS_DEBUG
	pdxlopMat->AssertValid(pdxlnMaterialize, false /* fValidateChildren */);
#endif

	return pdxlnMaterialize;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnSequence
//
//	@doc:
//		Create a DXL sequence node from an optimizer sequence expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnSequence
	(
	CExpression *pexprSequence,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprSequence);

	const ULONG ulArity = pexprSequence->UlArity();
	GPOS_ASSERT(0 < ulArity);

	// construct sequence node
	CDXLPhysicalSequence *pdxlopSequence = GPOS_NEW(m_pmp) CDXLPhysicalSequence(m_pmp);	
	CDXLNode *pdxlnSequence = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopSequence);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprSequence);
	pdxlnSequence->SetProperties(pdxlprop);

	// translate children
	DrgPdxln *pdrgpdxlnChildren = GPOS_NEW(m_pmp) DrgPdxln(m_pmp);
	
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = (*pexprSequence)[ul];
		
		DrgPcr *pdrgpcrChildOutput = NULL;
		if (ul == ulArity - 1)
		{
			// impose output columns on last child
			pdrgpcrChildOutput = pdrgpcr;
		}
		
		CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcrChildOutput, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);
		pdrgpdxlnChildren->Append(pdxlnChild);
	}
	
	// construct project list from the project list of the last child
	CDXLNode *pdxlnLastChild = (*pdrgpdxlnChildren)[ulArity - 1];
	CDXLNode *pdxlnProjListChild = (*pdxlnLastChild)[0];
	
	CDXLNode *pdxlnProjList = CTranslatorExprToDXLUtils::PdxlnProjListFromChildProjList(m_pmp, m_pcf, m_phmcrdxln, pdxlnProjListChild);
	pdxlnSequence->AddChild(pdxlnProjList);
	
	// add children
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChid = (*pdrgpdxlnChildren)[ul];
		pdxlnChid->AddRef();
		pdxlnSequence->AddChild(pdxlnChid);
	}
	
	pdrgpdxlnChildren->Release();
	
#ifdef GPOS_DEBUG
	pdxlopSequence->AssertValid(pdxlnSequence, false /* fValidateChildren */);
#endif

	return pdxlnSequence;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPartitionSelector
//
//	@doc:
//		Translate a partition selector into DXL
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPartitionSelector
	(
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	return PdxlnPartitionSelector(pexpr, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, NULL /*pexprScalarCond*/, NULL /*pdxlprop*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPartitionSelector
//
//	@doc:
//		Translate a partition selector into DXL
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPartitionSelector
	(
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	CExpression *pexprScalarCond,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CPhysicalPartitionSelector *popSelector = CPhysicalPartitionSelector::PopConvert(pexpr->Pop());

	CExpression *pexprScalar = popSelector->PexprCombinedPred();
	if (CUtils::FScalarConstTrue(pexprScalar))
	{
		return PdxlnPartitionSelectorExpand(pexpr, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, pexprScalarCond, pdxlprop);
	}

	return PdxlnPartitionSelectorFilter(pexpr, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, pexprScalarCond, pdxlprop);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPartitionSelectorDML
//
//	@doc:
//		Translate a DML partition selector into DXL
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPartitionSelectorDML
	(
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(1 == pexpr->UlArity());

	CExpression *pexprChild = (*pexpr)[0];
	CPhysicalPartitionSelectorDML *popSelector = CPhysicalPartitionSelectorDML::PopConvert(pexpr->Pop());

	// translate child
	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	// construct project list
	GPOS_ASSERT(1 <= pdxlnChild->UlArity());
	CDXLNode *pdxlnPrL = CTranslatorExprToDXLUtils::PdxlnPrLPartitionSelector
							(
							m_pmp,
							m_pmda,
							m_pcf,
							m_phmcrdxln,
							true, //fUseChildProjList
							(*pdxlnChild)[0],
							popSelector->PcrOid(),
							popSelector->UlPartLevels()
							);

	// translate filters
	CDXLNode *pdxlnEqFilters = NULL;
	CDXLNode *pdxlnFilters = NULL;
	CDXLNode *pdxlnResidual = NULL;
	TranslatePartitionFilters(pexpr, true /*fPassThrough*/, &pdxlnEqFilters, &pdxlnFilters, &pdxlnResidual);

	// since there is no propagation for DML, we create a const null expression
	const IMDTypeInt4 *pmdtypeint4 = m_pmda->PtMDType<IMDTypeInt4>();
	CDXLDatum *pdxldatumNull = pmdtypeint4->PdxldatumNull(m_pmp);
	CDXLNode *pdxlnPropagation = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarConstValue(m_pmp, pdxldatumNull));

	// true printable filter
	CDXLNode *pdxlnPrintable = CTranslatorExprToDXLUtils::PdxlnBoolConst(m_pmp, m_pmda, true /*fVal*/);

	// construct PartitionSelector node
	IMDId *pmdidRel = popSelector->Pmdid();
	pmdidRel->AddRef();

	CDXLNode *pdxlnSelector = CTranslatorExprToDXLUtils::PdxlnPartitionSelector
									(
									m_pmp,
									pmdidRel,
									popSelector->UlPartLevels(),
									0, // ulScanId
									Pdxlprop(pexpr),
									pdxlnPrL,
									pdxlnEqFilters,
									pdxlnFilters,
									pdxlnResidual,
									pdxlnPropagation,
									pdxlnPrintable,
									pdxlnChild
									);

#ifdef GPOS_DEBUG
	pdxlnSelector->Pdxlop()->AssertValid(pdxlnSelector, false /* fValidateChildren */);
#endif

	return pdxlnSelector;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPartFilterList
//
//	@doc:
//		Return a DXL part filter list. Can be used for the equality filters or
//		the general filters
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPartFilterList
	(
	CExpression *pexpr,
	BOOL fEqFilters
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CPhysicalPartitionSelector *popSelector = CPhysicalPartitionSelector::PopConvert(pexpr->Pop());

	CDXLNode *pdxlnFilters = NULL;
	if (fEqFilters)
	{
		pdxlnFilters = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOpList(m_pmp, CDXLScalarOpList::EdxloplistEqFilterList));
	}
	else
	{
		pdxlnFilters = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOpList(m_pmp, CDXLScalarOpList::EdxloplistFilterList));
	}

	const ULONG ulPartLevels = popSelector->UlPartLevels();
	GPOS_ASSERT(1 <= ulPartLevels);

	for (ULONG ul = 0; ul < ulPartLevels; ul++)
	{
		CExpression *pexprFilter = NULL;
		if (fEqFilters)
		{
			pexprFilter = popSelector->PexprEqFilter(ul);
		}
		else
		{
			pexprFilter = popSelector->PexprFilter(ul);
		}

		CDXLNode *pdxlnFilter = NULL;
		if (NULL == pexprFilter)
		{
			pdxlnFilter = CTranslatorExprToDXLUtils::PdxlnBoolConst(m_pmp, m_pmda, true /*fVal*/);
		}
		else
		{
			pdxlnFilter = PdxlnScalar(pexprFilter);
		}
		pdxlnFilters->AddChild(pdxlnFilter);
	}

	return pdxlnFilters;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPartitionSelectorExpand
//
//	@doc:
//		Translate an expand-based partition resolver into DXL
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPartitionSelectorExpand
	(
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	CExpression *pexprScalarCond,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(1 == pexpr->UlArity());

	CExpression *pexprChild = (*pexpr)[0];

	GPOS_ASSERT_IMP
		(
		NULL != pexprScalarCond, 
		(COperator::EopPhysicalDynamicTableScan == pexprChild->Pop()->Eopid() ||
		 COperator::EopPhysicalDynamicIndexScan == pexprChild->Pop()->Eopid() ||
		 COperator::EopPhysicalDynamicBitmapTableScan == pexprChild->Pop()->Eopid())
		&& "Inlining predicates only allowed in DynamicTableScan, DynamicIndexScan and DynamicBitmapTableScan"
		);
	
	CPhysicalPartitionSelector *popSelector = CPhysicalPartitionSelector::PopConvert(pexpr->Pop());
	const ULONG ulLevels = popSelector->UlPartLevels();
		
	// translate child
	CDXLNode *pdxlnChild = PdxlnPartitionSelectorChild(pexprChild, pexprScalarCond, pdxlprop, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
	
	// project list
	CDXLNode *pdxlnPrL = CTranslatorExprToDXLUtils::PdxlnPrLPartitionSelector
							(
							m_pmp,
							m_pmda,
							m_pcf,
							m_phmcrdxln,
							false, //fUseChildProjList
							NULL, //pdxlnPrLchild
							NULL, //pcrOid
							ulLevels
							);

	// translate filters
	CDXLNode *pdxlnEqFilters = NULL;
	CDXLNode *pdxlnFilters = NULL;
	CDXLNode *pdxlnResidual = NULL;
	TranslatePartitionFilters(pexpr, true /*fPassThrough*/, &pdxlnEqFilters, &pdxlnFilters, &pdxlnResidual);

	// construct propagation expression
	CPartIndexMap *ppimDrvd = m_pdpplan->Ppim();
	ULONG ulScanId = popSelector->UlScanId();
	CDXLNode *pdxlnPropagation = CTranslatorExprToDXLUtils::PdxlnPropExprPartitionSelector
									(
									m_pmp,
									m_pmda,
									m_pcf,
									ppimDrvd->FPartialScans(ulScanId),
									ppimDrvd->Ppartcnstrmap(ulScanId),
									popSelector->Pdrgpdrgpcr(),
									ulScanId
									);

	// translate printable filter
	CExpression *pexprPrintable = popSelector->PexprCombinedPred();
	GPOS_ASSERT(NULL != pexprPrintable);
	CDXLNode *pdxlnPrintable = PdxlnScalar(pexprPrintable);

	// construct PartitionSelector node
	IMDId *pmdidRel = popSelector->Pmdid();
	pmdidRel->AddRef();

	CDXLNode *pdxlnSelector = CTranslatorExprToDXLUtils::PdxlnPartitionSelector
									(
									m_pmp,
									pmdidRel,
									ulLevels,
									ulScanId,
									CTranslatorExprToDXLUtils::Pdxlprop(m_pmp),
									pdxlnPrL,
									pdxlnEqFilters,
									pdxlnFilters,
									pdxlnResidual,
									pdxlnPropagation,
									pdxlnPrintable
									);

	// construct sequence node
	CDXLPhysicalSequence *pdxlopSequence = GPOS_NEW(m_pmp) CDXLPhysicalSequence(m_pmp);
	CDXLNode *pdxlnSequence = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopSequence);

	CDXLPhysicalProperties *pdxlpropSeq = CTranslatorExprToDXLUtils::PdxlpropCopy(m_pmp, pdxlnChild);
	pdxlnSequence->SetProperties(pdxlpropSeq);
	
	// construct sequence's project list from the project list of the last child
	CDXLNode *pdxlnPrLChild = (*pdxlnChild)[0];
	CDXLNode *pdxlnPrLSequence = CTranslatorExprToDXLUtils::PdxlnProjListFromChildProjList(m_pmp, m_pcf, m_phmcrdxln, pdxlnPrLChild);
	
	pdxlnSequence->AddChild(pdxlnPrLSequence);
	pdxlnSequence->AddChild(pdxlnSelector);
	pdxlnSequence->AddChild(pdxlnChild);
	
#ifdef GPOS_DEBUG
	pdxlopSequence->AssertValid(pdxlnSequence, false /* fValidateChildren */);
#endif

	return pdxlnSequence;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPartitionSelectorFilter
//
//	@doc:
//		Translate a filter-based partition selector into DXL
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPartitionSelectorFilter
	(
	CExpression *pexpr,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML,
	CExpression *pexprScalarCond,
	CDXLPhysicalProperties *pdxlprop
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(1 == pexpr->UlArity());

	CPhysicalPartitionSelector *popSelector = CPhysicalPartitionSelector::PopConvert(pexpr->Pop());
	CPartIndexMap *ppimDrvd = m_pdpplan->Ppim();
	ULONG ulScanId = popSelector->UlScanId();
	ULONG ulLevels = popSelector->UlPartLevels();
	BOOL fPartialScans = ppimDrvd->FPartialScans(ulScanId);
	PartCnstrMap *ppartcnstrmap = ppimDrvd->Ppartcnstrmap(ulScanId);

	BOOL fPassThrough = FEqPartFiltersAllLevels(pexpr, false /*fCheckGeneralFilters*/) && !fPartialScans;
#ifdef GPOS_DEBUG
	BOOL fPoint = FEqPartFiltersAllLevels(pexpr, true /*fCheckGeneralFilters*/) && !fPartialScans;
	GPOS_ASSERT_IMP(!fPoint && fPartialScans, NULL != ppartcnstrmap);
#endif

	CExpression *pexprChild = (*pexpr)[0];

	GPOS_ASSERT_IMP
		(
		NULL != pexprScalarCond,
		(COperator::EopPhysicalDynamicTableScan == pexprChild->Pop()->Eopid() ||
		 COperator::EopPhysicalDynamicIndexScan == pexprChild->Pop()->Eopid() ||
		 COperator::EopPhysicalDynamicBitmapTableScan == pexprChild->Pop()->Eopid())
		&& "Inlining predicates only allowed in DynamicTableScan, DynamicIndexScan and DynamicBitmapTableScan"
		);

	// translate child
	CDXLNode *pdxlnChild = PdxlnPartitionSelectorChild(pexprChild, pexprScalarCond, pdxlprop, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
	CDXLNode *pdxlnPrLChild = (*pdxlnChild)[0];

	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexprChild->Pdp(CDrvdProp::EptRelational));
	// we add a sequence if the scan id is found below the resolver
	BOOL fNeedSequence = pdprel->Ppartinfo()->FContainsScanId(popSelector->UlScanId());

	// project list
	CDXLNode *pdxlnPrL = CTranslatorExprToDXLUtils::PdxlnPrLPartitionSelector
							(
							m_pmp,
							m_pmda,
							m_pcf,
							m_phmcrdxln,
							!fNeedSequence,
							pdxlnPrLChild,
							NULL /*pcrOid*/,
							ulLevels
							);

	// translate filters
	CDXLNode *pdxlnEqFilters = NULL;
	CDXLNode *pdxlnFilters = NULL;
	CDXLNode *pdxlnResidual = NULL;
	TranslatePartitionFilters(pexpr, fPassThrough, &pdxlnEqFilters, &pdxlnFilters, &pdxlnResidual);

	// construct propagation expression
	CDXLNode *pdxlnPropagation = CTranslatorExprToDXLUtils::PdxlnPropExprPartitionSelector
									(
									m_pmp,
									m_pmda,
									m_pcf,
									!fPassThrough && fPartialScans, //fConditional
									ppartcnstrmap,
									popSelector->Pdrgpdrgpcr(),
									popSelector->UlScanId()
									);

	// translate printable filter
	CExpression *pexprPrintable = popSelector->PexprCombinedPred();
	GPOS_ASSERT(NULL != pexprPrintable);
	CDXLNode *pdxlnPrintable = PdxlnScalar(pexprPrintable);

	// construct PartitionSelector node
	IMDId *pmdidRel = popSelector->Pmdid();
	pmdidRel->AddRef();

	CDXLNode *pdxlnSelectorChild = NULL;
	if (!fNeedSequence)
	{
		pdxlnSelectorChild = pdxlnChild;
	}

	CDXLNode *pdxlnSelector = CTranslatorExprToDXLUtils::PdxlnPartitionSelector
									(
									m_pmp,
									pmdidRel,
									ulLevels,
									ulScanId,
									CTranslatorExprToDXLUtils::Pdxlprop(m_pmp),
									pdxlnPrL,
									pdxlnEqFilters,
									pdxlnFilters,
									pdxlnResidual,
									pdxlnPropagation,
									pdxlnPrintable,
									pdxlnSelectorChild
									);

	CDXLNode *pdxlnReturned = pdxlnSelector;
	if (fNeedSequence)
	{
		CDXLPhysicalSequence *pdxlopSequence = GPOS_NEW(m_pmp) CDXLPhysicalSequence(m_pmp);
		CDXLNode *pdxlnSequence = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopSequence);
		CDXLPhysicalProperties *pdxlpropSeq = CTranslatorExprToDXLUtils::PdxlpropCopy(m_pmp, pdxlnChild);
		pdxlnSequence->SetProperties(pdxlpropSeq);

		// construct sequence's project list from the project list of the last child
		CDXLNode *pdxlnPrLSequence = CTranslatorExprToDXLUtils::PdxlnProjListFromChildProjList(m_pmp, m_pcf, m_phmcrdxln, pdxlnPrLChild);

		pdxlnSequence->AddChild(pdxlnPrLSequence);
		pdxlnSequence->AddChild(pdxlnSelector);
		pdxlnSequence->AddChild(pdxlnChild);

		pdxlnReturned = pdxlnSequence;
	}

#ifdef GPOS_DEBUG
	pdxlnReturned->Pdxlop()->AssertValid(pdxlnReturned, false /* fValidateChildren */);
#endif

	return pdxlnReturned;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::FEqPartFiltersAllLevels
//
//	@doc:
//		Check whether the given partition selector has equality filters on
//		all partitioning levels. If fCheckGeneralFilters is true then the function
//		checks the general filters as well. Otherwise, only the EqFilters are considered
//
//---------------------------------------------------------------------------
BOOL
CTranslatorExprToDXL::FEqPartFiltersAllLevels
	(
	CExpression *pexpr,
	BOOL fCheckGeneralFilters
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CPhysicalPartitionSelector *popSelector = CPhysicalPartitionSelector::PopConvert(pexpr->Pop());
	const ULONG ulPartLevels = popSelector->UlPartLevels();
	GPOS_ASSERT(1 <= ulPartLevels);

	for (ULONG ul = 0; ul < ulPartLevels; ul++)
	{
		CExpression *pexprEqFilter = popSelector->PexprEqFilter(ul);
		CExpression *pexprFilter = popSelector->PexprFilter(ul);

		if (NULL == pexprEqFilter &&
			(!fCheckGeneralFilters || NULL == pexprFilter || !CPredicateUtils::FConjunctionOfEqComparisons(m_pmp, pexprFilter)))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::TranslatePartitionFilters
//
//	@doc:
//		Translate partition selector filters
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::TranslatePartitionFilters
	(
	CExpression *pexprPartSelector,
	BOOL fPassThrough,
	CDXLNode **ppdxlnEqFilters,		// output: translated equality filters
	CDXLNode **ppdxlnFilters,		// output: translated non-equality filters
	CDXLNode **ppdxlnResidual		// output: translated residual filters
	)
{
	GPOS_ASSERT(NULL != pexprPartSelector);

	if (fPassThrough)
	{
		*ppdxlnEqFilters = PdxlnPartFilterList(pexprPartSelector, true /*fEqFilters*/);
		*ppdxlnFilters = PdxlnPartFilterList(pexprPartSelector, false /*fEqFilters*/);

		CPhysicalPartitionSelector *popSelector = CPhysicalPartitionSelector::PopConvert(pexprPartSelector->Pop());
		CExpression *pexprResidual = popSelector->PexprResidualPred();
		if (NULL == pexprResidual)
		{
			*ppdxlnResidual = CTranslatorExprToDXLUtils::PdxlnBoolConst(m_pmp, m_pmda, true /*fVal*/);
		}
		else
		{
			*ppdxlnResidual = PdxlnScalar(pexprResidual);
		}

		return;
	}

	ConstructLevelFiltersPartitionSelectorRange(pexprPartSelector, ppdxlnEqFilters, ppdxlnFilters);

	// TODO:  - Apr 11, 2014; translate the residual filter. Take into account
	// that this might be an arbitrary scalar expression on multiple part keys. Right
	// now we assume no residual filter in this case
	*ppdxlnResidual = CTranslatorExprToDXLUtils::PdxlnBoolConst(m_pmp, m_pmda, true /*fVal*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::ConstructLevelFiltersPartitionSelectorRange
//
//	@doc:
// 		Construct the level filter lists for a range-based partition selector
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::ConstructLevelFiltersPartitionSelectorRange
	(
	CExpression *pexprPartSelector,
	CDXLNode **ppdxlnEqFilters,
	CDXLNode **ppdxlnFilters
	)
{
	GPOS_ASSERT(NULL != pexprPartSelector);
	CPhysicalPartitionSelector *popSelector = CPhysicalPartitionSelector::PopConvert(pexprPartSelector->Pop());

	const ULONG ulPartLevels = popSelector->UlPartLevels();
	GPOS_ASSERT(1 <= ulPartLevels);

	DrgDrgPcr *pdrgpdrgpcrPartKeys = popSelector->Pdrgpdrgpcr();
	CBitSet *pbsDefaultParts = CUtils::Pbs(m_pmp, m_pmda->Pmdrel(popSelector->Pmdid())->Pmdpartcnstr()->PdrgpulDefaultParts());

	*ppdxlnFilters = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOpList(m_pmp, CDXLScalarOpList::EdxloplistFilterList));
	*ppdxlnEqFilters = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOpList(m_pmp, CDXLScalarOpList::EdxloplistEqFilterList));

	for (ULONG ulLevel = 0; ulLevel < ulPartLevels; ulLevel++)
	{
		CColRef *pcrPartKey = CUtils::PcrExtractPartKey(pdrgpdrgpcrPartKeys, ulLevel);
		IMDId *pmdidTypePartKey = pcrPartKey->Pmdtype()->Pmdid();

		CDXLNode *pdxlnFilter = NULL;
		BOOL fDefaultPartition = pbsDefaultParts->FBit(ulLevel);

		BOOL fLTComparison = false;
		BOOL fGTComparison = false;
		BOOL fEQComparison = false;

		// check if there is an equality filter on current level
		CExpression *pexprEqFilter = popSelector->PexprEqFilter(ulLevel);
		if (NULL != pexprEqFilter)
		{
			CDXLNode *pdxlnEq = PdxlnScalar(pexprEqFilter);
			IMDId *pmdidTypeOther = CScalar::PopConvert(pexprEqFilter->Pop())->PmdidType();
			fEQComparison = true;

			pdxlnFilter = CTranslatorExprToDXLUtils::PdxlnRangeFilterEqCmp
							(
							m_pmp,
							m_pmda,
							pdxlnEq,
							pmdidTypePartKey,
							pmdidTypeOther,
							NULL /*pmdidTypeCastExpr*/,
							NULL /*pmdidCastFunc*/,
							ulLevel
							);
		}

		// check general filters on current level
		CExpression *pexprFilter = popSelector->PexprFilter(ulLevel);
		if (NULL != pexprFilter)
		{
			DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(m_pmp, pexprFilter);
			const ULONG ulLength = pdrgpexprConjuncts->UlLength();

			for (ULONG ul = 0; ul < ulLength; ul++)
			{
				CDXLNode *pdxlnScCmp = PdxlnPredOnPartKey
										(
										(*pdrgpexprConjuncts)[ul],
										pcrPartKey,
										pmdidTypePartKey,
										ulLevel,
										&fLTComparison,
										&fGTComparison,
										&fEQComparison
										);

				pdxlnFilter = CTranslatorExprToDXLUtils::PdxlnCombineBoolean(m_pmp, pdxlnFilter, pdxlnScCmp, Edxland);
			}

			pdrgpexprConjuncts->Release();
		}

		if (NULL != pdxlnFilter)
		{
			CDXLNode *pdxlnDefaultAndOpenEnded = CTranslatorExprToDXLUtils::PdxlnRangeFilterDefaultAndOpenEnded
										(
										m_pmp,
										ulLevel,
										fLTComparison,
										fGTComparison,
										fEQComparison,
										fDefaultPartition
										);

			pdxlnFilter = CTranslatorExprToDXLUtils::PdxlnCombineBoolean(m_pmp, pdxlnFilter, pdxlnDefaultAndOpenEnded, Edxlor);
		}

		if (NULL == pdxlnFilter)
		{
			pdxlnFilter = CTranslatorExprToDXLUtils::PdxlnBoolConst(m_pmp, m_pmda, true /*fVal*/);
		}

		(*ppdxlnFilters)->AddChild(pdxlnFilter);
		(*ppdxlnEqFilters)->AddChild(CTranslatorExprToDXLUtils::PdxlnBoolConst(m_pmp, m_pmda, true /*fVal*/));
	}

	pbsDefaultParts->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPredOnPartKey
//
//	@doc:
// 		Translate a general predicate on a part key and update the various
//		comparison type flags accordingly
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPredOnPartKey
	(
	CExpression *pexprPred,
	CColRef *pcrPartKey,
	IMDId *pmdidTypePartKey,
	ULONG ulPartLevel,
	BOOL *pfLTComparison,	// input/output
	BOOL *pfGTComparison,	// input/output
	BOOL *pfEQComparison	// input/output
	)
{
	if (CPredicateUtils::FComparison(pexprPred))
	{
		return PdxlnScCmpPartKey(pexprPred, pcrPartKey, pmdidTypePartKey, ulPartLevel, pfLTComparison, pfGTComparison, pfEQComparison);
	}
	
	if (CUtils::FScalarNullTest(pexprPred))
	{
#ifdef GPOS_DEBUG
		CExpression *pexprChild = (*pexprPred)[0];
		GPOS_ASSERT(CUtils::FScalarIdent(pexprChild, pcrPartKey) || CUtils::FBinaryCoercibleCastedScId(pexprChild, pcrPartKey));
#endif //GPOS_DEBUG

		return PdxlnScNullTestPartKey(pmdidTypePartKey, ulPartLevel, true /*fIsNull*/);
	}

	if (CUtils::FScalarNotNull(pexprPred))
	{
#ifdef GPOS_DEBUG
		CExpression *pexprIsNull = (*pexprPred)[0];
		CExpression *pexprChild = (*pexprIsNull)[0];
		GPOS_ASSERT(CUtils::FScalarIdent(pexprChild, pcrPartKey) || CUtils::FBinaryCoercibleCastedScId(pexprChild, pcrPartKey));
#endif //GPOS_DEBUG

		*pfEQComparison = true;
		return PdxlnScNullTestPartKey(pmdidTypePartKey, ulPartLevel, false /*fIsNull*/);
	}

	GPOS_ASSERT(CPredicateUtils::FOr(pexprPred) || CPredicateUtils::FAnd(pexprPred));
	
	return PdxlnConjDisjOnPartKey(pexprPred, pcrPartKey, pmdidTypePartKey, ulPartLevel, pfLTComparison, pfGTComparison, pfEQComparison);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnConjDisjOnPartKey
//
//	@doc:
// 		Translate a conjunctive or disjunctive predicate on a part key and update
//		the various comparison type flags accordingly
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnConjDisjOnPartKey
	(
	CExpression *pexprPred,
	CColRef *pcrPartKey,
	IMDId *pmdidTypePartKey,
	ULONG ulPartLevel,
	BOOL *pfLTComparison,	// input/output
	BOOL *pfGTComparison,	// input/output
	BOOL *pfEQComparison	// input/output
	)
{
	GPOS_ASSERT(CPredicateUtils::FOr(pexprPred) || CPredicateUtils::FAnd(pexprPred));
	
	DrgPexpr *pdrgpexprChildren = NULL;
	EdxlBoolExprType edxlbet = Edxland;
	if (CPredicateUtils::FAnd(pexprPred))
	{
		pdrgpexprChildren = CPredicateUtils::PdrgpexprConjuncts(m_pmp, pexprPred);
	}
	else
	{
		pdrgpexprChildren = CPredicateUtils::PdrgpexprDisjuncts(m_pmp, pexprPred);
		edxlbet = Edxlor;
	}

	const ULONG ulChildren = pdrgpexprChildren->UlLength();
	
	CDXLNode *pdxlnPred = NULL;
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pdrgpexprChildren)[ul];
		CDXLNode *pdxlnChild = PdxlnPredOnPartKey(pexprChild, pcrPartKey, pmdidTypePartKey, ulPartLevel, pfLTComparison, pfGTComparison, pfEQComparison);
		
		if (NULL == pdxlnPred)
		{
			pdxlnPred = pdxlnChild;
		}
		else
		{
			pdxlnPred = CTranslatorExprToDXLUtils::PdxlnCombineBoolean(m_pmp, pdxlnPred, pdxlnChild, edxlbet);
		}
	}
	
	pdrgpexprChildren->Release();
	
	return pdxlnPred;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScCmpPartKey
//
//	@doc:
// 		Translate a scalar comparison on a part key and update the various
//		comparison type flags accordingly
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScCmpPartKey
	(
	CExpression *pexprScCmp,
	CColRef *pcrPartKey,
	IMDId *pmdidTypePartKey,
	ULONG ulPartLevel,
	BOOL *pfLTComparison,	// input/output
	BOOL *pfGTComparison,	// input/output
	BOOL *pfEQComparison	// input/output
	)
{
	GPOS_ASSERT(CPredicateUtils::FComparison(pexprScCmp));

	// extract components
	CExpression *pexprPartKey = NULL;
	CExpression *pexprOther = NULL;
	IMDType::ECmpType ecmpt = IMDType::EcmptOther;

	CPredicateUtils::ExtractComponents(pexprScCmp, pcrPartKey, &pexprPartKey, &pexprOther, &ecmpt);

	*pfLTComparison = *pfLTComparison || (IMDType::EcmptL == ecmpt) || (IMDType::EcmptLEq == ecmpt);
	*pfGTComparison = *pfGTComparison || (IMDType::EcmptG == ecmpt) || (IMDType::EcmptGEq == ecmpt);
	*pfEQComparison = *pfEQComparison || IMDType::EcmptEq == ecmpt;

	GPOS_ASSERT(NULL != pexprPartKey && NULL != pexprOther);
	GPOS_ASSERT(IMDType::EcmptOther != ecmpt);

	CDXLNode *pdxlnOther = PdxlnScalar(pexprOther);
	IMDId *pmdidTypeOther = CScalar::PopConvert(pexprOther->Pop())->PmdidType();
	IMDId *pmdidTypeCastExpr = NULL;
	IMDId *pmdidCastFunc = NULL;

	CExpression *pexprNewPartKey = pexprPartKey;

	// If the pexprPartKey is not comparable with pexprOther, but can be casted to pexprOther,
	// and not yet casted, then we add a cast on top of pexprPartKey.
	if (!CMDAccessorUtils::FCmpExists(m_pmda, pmdidTypePartKey, pmdidTypeOther, ecmpt)
		&& CMDAccessorUtils::FCastExists(m_pmda, pmdidTypePartKey, pmdidTypeOther)
		&& COperator::EopScalarCast != pexprPartKey->Pop()->Eopid())
	{
		pexprNewPartKey = CUtils::PexprCast(m_pmp, m_pmda, pexprPartKey, pmdidTypeOther);
		pexprPartKey->Release();
	}

	CTranslatorExprToDXLUtils::ExtractCastMdids(pexprNewPartKey->Pop(), &pmdidTypeCastExpr, &pmdidCastFunc);

	return CTranslatorExprToDXLUtils::PdxlnRangeFilterScCmp
							(
							m_pmp,
							m_pmda,
							pdxlnOther,
							pmdidTypePartKey,
							pmdidTypeOther,
							pmdidTypeCastExpr,
							pmdidCastFunc,
							ecmpt,
							ulPartLevel
							);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScNullTestPartKey
//
//	@doc:
// 		Translate a scalar null test on a part key
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScNullTestPartKey
	(
	IMDId *pmdidTypePartKey,
	ULONG ulPartLevel,
	BOOL fIsNull
	)
{
	pmdidTypePartKey->AddRef();
	CDXLNode *pdxlnPredicateMin = GPOS_NEW(m_pmp) CDXLNode
							(
							m_pmp,
							GPOS_NEW(m_pmp) CDXLScalarNullTest(m_pmp, fIsNull),
							GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarPartBound(m_pmp, ulPartLevel, pmdidTypePartKey, true /*fLower*/))
							);

	pmdidTypePartKey->AddRef();
	CDXLNode *pdxlnPredicateMax = GPOS_NEW(m_pmp) CDXLNode
							(
							m_pmp,
							GPOS_NEW(m_pmp) CDXLScalarNullTest(m_pmp, fIsNull),
							GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarPartBound(m_pmp, ulPartLevel, pmdidTypePartKey, false /*fLower*/))
							);

	// construct the conjunction of the predicate for the lower and upper bounds
	CDXLNode *pdxlnNullTests = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarBoolExpr(m_pmp, Edxland), pdxlnPredicateMin, pdxlnPredicateMax);

	// AND that with the following: !(default || min_open || max_open)
	CDXLNode *pdxlnDefaultOrOpenEnded = CTranslatorExprToDXLUtils::PdxlnRangeFilterDefaultAndOpenEnded
								(
								m_pmp,
								ulPartLevel,
								true, //fLTComparison
								true, //fGTComparison
								false, //fEQComparison
								true //fDefaultPartition
								);

	CDXLNode *pdxlnNotDefaultOrOpenEnded = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarBoolExpr(m_pmp, Edxlnot), pdxlnDefaultOrOpenEnded);

	return GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarBoolExpr(m_pmp, Edxland), pdxlnNotDefaultOrOpenEnded, pdxlnNullTests);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnPartitionSelectorChild
//
//	@doc:
// 		Translate the child of a partition selector expression, pushing the given
//		scalar predicate if available
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnPartitionSelectorChild
	(
	CExpression *pexprChild,
	CExpression *pexprScalarCond,
	CDXLPhysicalProperties *pdxlprop,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT_IFF(NULL != pexprScalarCond, NULL != pdxlprop);
	
	if (NULL == pexprScalarCond)
	{
		return Pdxln(pexprChild, pdrgpcr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, true, false);
	}
	
	switch(pexprChild->Pop()->Eopid())
	{
		case COperator::EopPhysicalDynamicTableScan:
			return PdxlnDynamicTableScan(pexprChild, pdrgpcr, pdrgpdsBaseTables, pexprScalarCond, pdxlprop);
		case COperator::EopPhysicalDynamicIndexScan:
			return PdxlnIndexScanWithInlinedCondition(pexprChild, pexprScalarCond, pdxlprop, pdrgpcr, pdrgpdsBaseTables);
		default:
			GPOS_ASSERT(COperator::EopPhysicalDynamicBitmapTableScan == pexprChild->Pop()->Eopid());
			return PdxlnDynamicBitmapTableScan(pexprChild, pdrgpcr, pdrgpdsBaseTables, pexprScalarCond, pdxlprop);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnDML
//
//	@doc:
//		Translate a DML operator
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnDML
	(
	CExpression *pexpr,
	DrgPcr *,// pdrgpcr
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(1 == pexpr->UlArity());
	
	ULONG ulAction = 0;
	ULONG ulOid = 0;
	ULONG ulCtid = 0;
	ULONG ulSegmentId = 0;

	// extract components
	CPhysicalDML *popDML = CPhysicalDML::PopConvert(pexpr->Pop());
	*pfDML = false;
	if (IMDId::EmdidGPDBCtas == popDML->Ptabdesc()->Pmdid()->Emdidt())
	{
		return PdxlnCTAS(pexpr, pdrgpdsBaseTables, pulNonGatherMotions, pfDML);
	}
	
	EdxlDmlType edxldmltype = Edxldmloptype(popDML->Edmlop());

	CExpression *pexprChild = (*pexpr)[0];
	CTableDescriptor *ptabdesc = popDML->Ptabdesc();
	DrgPcr *pdrgpcrSource = popDML->PdrgpcrSource();

	CColRef *pcrAction = popDML->PcrAction();
	GPOS_ASSERT(NULL != pcrAction);
	ulAction = pcrAction->UlId();

	CColRef *pcrOid = popDML->PcrTableOid();
	GPOS_ASSERT(NULL != pcrOid);
	ulOid = pcrOid->UlId();

	CColRef *pcrCtid = popDML->PcrCtid();
	CColRef *pcrSegmentId = popDML->PcrSegmentId();
	if (NULL != pcrCtid)
	{
		GPOS_ASSERT(NULL != pcrSegmentId);
		ulCtid = pcrCtid->UlId();
		ulSegmentId = pcrSegmentId->UlId();
	}
	
	CColRef *pcrTupleOid = popDML->PcrTupleOid();
	ULONG ulTupleOid = 0;
	BOOL fPreserveOids = false;
	if (NULL != pcrTupleOid)
	{
		fPreserveOids = true;
		ulTupleOid = pcrTupleOid->UlId();
	}
	
	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcrSource, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	CDXLTableDescr *pdxltabdesc = Pdxltabdesc(ptabdesc, NULL /*pdrgpcrOutput*/);
	DrgPul *pdrgpul = CUtils::Pdrgpul(m_pmp, pdrgpcrSource);
	
	CDXLDirectDispatchInfo *pdxlddinfo = Pdxlddinfo(pexpr);
	CDXLPhysicalDML *pdxlopDML = GPOS_NEW(m_pmp) CDXLPhysicalDML
									(
									m_pmp, 
									edxldmltype, 
									pdxltabdesc, 
									pdrgpul, 
									ulAction, 
									ulOid, 
									ulCtid, 
									ulSegmentId, 
									fPreserveOids, 
									ulTupleOid,
									pdxlddinfo,
									popDML->FInputSorted()
									);
	
	// project list
	CColRefSet *pcrsOutput = pexpr->Prpp()->PcrsRequired();
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcrSource);

	CDXLNode *pdxlnDML = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopDML);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexpr);
	pdxlnDML->SetProperties(pdxlprop);
	
	pdxlnDML->AddChild(pdxlnPrL);
	pdxlnDML->AddChild(pdxlnChild);
	
#ifdef GPOS_DEBUG
	pdxlnDML->Pdxlop()->AssertValid(pdxlnDML, false /* fValidateChildren */);
#endif
	*pfDML = true;

	return pdxlnDML;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnCTAS
//
//	@doc:
//		Translate a CTAS expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnCTAS
	(
	CExpression *pexpr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(1 == pexpr->UlArity());
	
	CPhysicalDML *popDML = CPhysicalDML::PopConvert(pexpr->Pop());
	GPOS_ASSERT(CLogicalDML::EdmlInsert == popDML->Edmlop());
	
	CExpression *pexprChild = (*pexpr)[0];
	CTableDescriptor *ptabdesc = popDML->Ptabdesc();
	DrgPcr *pdrgpcrSource = popDML->PdrgpcrSource();
	CMDRelationCtasGPDB *pmdrel = (CMDRelationCtasGPDB *) m_pmda->Pmdrel(ptabdesc->Pmdid());
	
	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcrSource, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, true /*fRemap*/, true /*fRoot*/);

	DrgPul *pdrgpul = CUtils::Pdrgpul(m_pmp, pdrgpcrSource);
	
	pmdrel->Pdxlctasopt()->AddRef();
	
	const ULONG ulColumns = ptabdesc->UlColumns();

	DrgPi *pdrgpiVarTypeMod = pmdrel->PdrgpiVarTypeMod();
	GPOS_ASSERT(ulColumns == pdrgpiVarTypeMod->UlLength());

	// translate col descriptors
	DrgPdxlcd *pdrgpdxlcd = GPOS_NEW(m_pmp) DrgPdxlcd(m_pmp);
	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		const CColumnDescriptor *pcd = ptabdesc->Pcoldesc(ul);
		
		CMDName *pmdnameCol = GPOS_NEW(m_pmp) CMDName(m_pmp, pcd->Name().Pstr());
		CColRef *pcr = m_pcf->PcrCreate(pcd->Pmdtype(), pcd->Name());

		// use the col ref id for the corresponding output output column as 
		// colid for the dxl column
		CMDIdGPDB *pmdidColType = CMDIdGPDB::PmdidConvert(pcr->Pmdtype()->Pmdid());
		pmdidColType->AddRef();
		
		CDXLColDescr *pdxlcd = GPOS_NEW(m_pmp) CDXLColDescr
											(
											m_pmp,
											pmdnameCol,
											pcr->UlId(),
											pcd->IAttno(),
											pmdidColType,
											false /* fdropped */,
											pcd->UlWidth()
											);
		
		pdrgpdxlcd->Append(pdxlcd);		
	}
	
	DrgPul *pdrgpulDistr = NULL;
	if (IMDRelation::EreldistrHash == pmdrel->Ereldistribution())
	{
		pdrgpulDistr = GPOS_NEW(m_pmp) DrgPul(m_pmp);
		const ULONG ulDistrCols = pmdrel->UlDistrColumns();
		for (ULONG ul = 0; ul < ulDistrCols; ul++)
		{
			const IMDColumn *pmdcol = pmdrel->PmdcolDistrColumn(ul);
			INT iAttno = pmdcol->IAttno();
			GPOS_ASSERT(0 < iAttno);
			pdrgpulDistr->Append(GPOS_NEW(m_pmp) ULONG(iAttno - 1));
		}
	}
	
	CMDName *pmdnameSchema = NULL;
	if (NULL != pmdrel->PmdnameSchema())
	{
		pmdnameSchema = GPOS_NEW(m_pmp) CMDName(m_pmp, pmdrel->PmdnameSchema()->Pstr());
	}

	pdrgpiVarTypeMod->AddRef();
	CDXLPhysicalCTAS *pdxlopCTAS = GPOS_NEW(m_pmp) CDXLPhysicalCTAS
									(
									m_pmp, 
									pmdnameSchema,
									GPOS_NEW(m_pmp) CMDName(m_pmp, pmdrel->Mdname().Pstr()),
									pdrgpdxlcd, 
									pmdrel->Pdxlctasopt(),
									pmdrel->Ereldistribution(),
									pdrgpulDistr,
									pmdrel->FTemporary(),
									pmdrel->FHasOids(),
									pmdrel->Erelstorage(),
									pdrgpul,
									pdrgpiVarTypeMod
									);
	
	CDXLNode *pdxlnCTAS = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopCTAS);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexpr);
	pdxlnCTAS->SetProperties(pdxlprop);
	
	CColRefSet *pcrsOutput = pexpr->Prpp()->PcrsRequired();
	CDXLNode *pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcrSource);

	pdxlnCTAS->AddChild(pdxlnPrL);
	pdxlnCTAS->AddChild(pdxlnChild);
	
#ifdef GPOS_DEBUG
	pdxlnCTAS->Pdxlop()->AssertValid(pdxlnCTAS, false /* fValidateChildren */);
#endif
	return pdxlnCTAS;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Pdxlddinfo
//
//	@doc:
//		Return the direct dispatch info spec for the possible values of the distribution
//		key in a DML insert statement. Returns NULL if values are not constant.
//
//---------------------------------------------------------------------------
CDXLDirectDispatchInfo *
CTranslatorExprToDXL::Pdxlddinfo
	(
	CExpression *pexprDML
	)
{
	GPOS_ASSERT(NULL != pexprDML);

	CPhysicalDML *popDML = CPhysicalDML::PopConvert(pexprDML->Pop());
	CTableDescriptor *ptabdesc = popDML->Ptabdesc();
	const DrgPcoldesc *pdrgpcoldescDist = ptabdesc->PdrgpcoldescDist();
	
	if (CLogicalDML::EdmlInsert != popDML->Edmlop() || 
		IMDRelation::EreldistrHash != ptabdesc->Ereldistribution() ||
		1 < pdrgpcoldescDist->UlLength())
	{
		// directed dispatch only supported for insert statements on hash-distributed tables 
		// with a single distribution column
		return NULL;
	}
	
	
	GPOS_ASSERT(1 == pdrgpcoldescDist->UlLength());	
	CColumnDescriptor *pcoldesc = (*pdrgpcoldescDist)[0];
	ULONG ulPos = ptabdesc->UlPos(pcoldesc, ptabdesc->Pdrgpcoldesc());	
	GPOS_ASSERT(ulPos < ptabdesc->Pdrgpcoldesc()->UlLength() && "Column not found");
		
	CColRef *pcrDistrCol = (*popDML->PdrgpcrSource())[ulPos];
	CPropConstraint *ppc = CDrvdPropRelational::Pdprel((*pexprDML)[0]->Pdp(CDrvdProp::EptRelational))->Ppc();

	if (NULL == ppc->Pcnstr())
	{
		return NULL;
	}

	CConstraint *pcnstrDistrCol = ppc->Pcnstr()->Pcnstr(m_pmp, pcrDistrCol);
	if (!CPredicateUtils::FConstColumn(pcnstrDistrCol, pcrDistrCol))
	{
		CRefCount::SafeRelease(pcnstrDistrCol);
		return NULL;
	}
	
	GPOS_ASSERT(CConstraint::EctInterval == pcnstrDistrCol->Ect());
	
	CConstraintInterval *pci = dynamic_cast<CConstraintInterval *>(pcnstrDistrCol);
	GPOS_ASSERT(1 >= pci->Pdrgprng()->UlLength());
	
	DrgPdxldatum *pdrgpdxldatum = GPOS_NEW(m_pmp) DrgPdxldatum(m_pmp);
	CDXLDatum *pdxldatum = NULL;
	
	if (1 == pci->Pdrgprng()->UlLength())
	{
		const CRange *prng = (*pci->Pdrgprng())[0];
		pdxldatum = CTranslatorExprToDXLUtils::Pdxldatum(m_pmp, m_pmda, prng->PdatumLeft());
	}
	else
	{
		GPOS_ASSERT(pci->FIncludesNull());
		pdxldatum = pcrDistrCol->Pmdtype()->PdxldatumNull(m_pmp);
	}
	
	pdrgpdxldatum->Append(pdxldatum);

	pcnstrDistrCol->Release(); 
	
	DrgPdrgPdxldatum *pdrgpdrgpdxldatum = GPOS_NEW(m_pmp) DrgPdrgPdxldatum(m_pmp);
	pdrgpdrgpdxldatum->Append(pdrgpdxldatum);
	return GPOS_NEW(m_pmp) CDXLDirectDispatchInfo(pdrgpdrgpdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnSplit
//
//	@doc:
//		Translate a split operator
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnSplit
	(
	CExpression *pexpr,
	DrgPcr *, // pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(2 == pexpr->UlArity());

	ULONG ulAction = 0;
	ULONG ulCtid = 0;
	ULONG ulSegmentId = 0;
	ULONG ulTupleOid = 0;

	// extract components
	CPhysicalSplit *popSplit = CPhysicalSplit::PopConvert(pexpr->Pop());

	CExpression *pexprChild = (*pexpr)[0];
	CExpression *pexprProjList = (*pexpr)[1];

	CColRef *pcrAction = popSplit->PcrAction();
	GPOS_ASSERT(NULL != pcrAction);
	ulAction = pcrAction->UlId();

	CColRef *pcrCtid = popSplit->PcrCtid();
	GPOS_ASSERT(NULL != pcrCtid);
	ulCtid = pcrCtid->UlId();

	CColRef *pcrSegmentId = popSplit->PcrSegmentId();
	GPOS_ASSERT(NULL != pcrSegmentId);
	ulSegmentId = pcrSegmentId->UlId();
	
	CColRef *pcrTupleOid = popSplit->PcrTupleOid();
	BOOL fPreserveOids = false;
	if (NULL != pcrTupleOid)
	{
		fPreserveOids = true;
		ulTupleOid = pcrTupleOid->UlId();
	}

	DrgPcr *pdrgpcrDelete = popSplit->PdrgpcrDelete();
	DrgPul *pdrgpulDelete = CUtils::Pdrgpul(m_pmp, pdrgpcrDelete);

	DrgPcr *pdrgpcrInsert = popSplit->PdrgpcrInsert();
	DrgPul *pdrgpulInsert = CUtils::Pdrgpul(m_pmp, pdrgpcrInsert);

	CColRefSet *pcrsRequired = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrsRequired->Include(pdrgpcrInsert);
	pcrsRequired->Include(pdrgpcrDelete);
	DrgPcr *pdrgpcrRequired = pcrsRequired->Pdrgpcr(m_pmp);

	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcrRequired, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, true /*fRemap*/, false /*fRoot*/);
	pdrgpcrRequired->Release();
	pcrsRequired->Release();

	CDXLPhysicalSplit *pdxlopSplit = GPOS_NEW(m_pmp) CDXLPhysicalSplit
													(
													m_pmp,
													pdrgpulDelete,
													pdrgpulInsert,
													ulAction,
													ulCtid,
													ulSegmentId,
													fPreserveOids,
													ulTupleOid
													);
	
	// project list
	CColRefSet *pcrsOutput = pexpr->Prpp()->PcrsRequired();
	CDXLNode *pdxlnPrL = PdxlnProjList(pexprProjList, pcrsOutput, pdrgpcrInsert);
	
	CDXLNode *pdxlnSplit = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopSplit);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexpr);
	pdxlnSplit->SetProperties(pdxlprop);
	
	pdxlnSplit->AddChild(pdxlnPrL);
	pdxlnSplit->AddChild(pdxlnChild);

#ifdef GPOS_DEBUG
	pdxlnSplit->Pdxlop()->AssertValid(pdxlnSplit, false /* fValidateChildren */);
#endif
	return pdxlnSplit;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnRowTrigger
//
//	@doc:
//		Translate a row trigger operator
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnRowTrigger
	(
	CExpression *pexpr,
	DrgPcr *, // pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(1 == pexpr->UlArity());

	// extract components
	CPhysicalRowTrigger *popRowTrigger = CPhysicalRowTrigger::PopConvert(pexpr->Pop());

	CExpression *pexprChild = (*pexpr)[0];

	IMDId *pmdidRel = popRowTrigger->PmdidRel();
	pmdidRel->AddRef();

	INT iType = popRowTrigger->IType();

	CColRefSet *pcrsRequired = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	DrgPul *pdrgpulOld = NULL;
	DrgPul *pdrgpulNew = NULL;

	DrgPcr *pdrgpcrOld = popRowTrigger->PdrgpcrOld();
	if (NULL != pdrgpcrOld)
	{
		pdrgpulOld = CUtils::Pdrgpul(m_pmp, pdrgpcrOld);
		pcrsRequired->Include(pdrgpcrOld);
	}

	DrgPcr *pdrgpcrNew = popRowTrigger->PdrgpcrNew();
	if (NULL != pdrgpcrNew)
	{
		pdrgpulNew = CUtils::Pdrgpul(m_pmp, pdrgpcrNew);
		pcrsRequired->Include(pdrgpcrNew);
	}

	DrgPcr *pdrgpcrRequired = pcrsRequired->Pdrgpcr(m_pmp);
	CDXLNode *pdxlnChild = Pdxln(pexprChild, pdrgpcrRequired, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);
	pdrgpcrRequired->Release();
	pcrsRequired->Release();

	CDXLPhysicalRowTrigger *pdxlopRowTrigger = GPOS_NEW(m_pmp) CDXLPhysicalRowTrigger
													(
													m_pmp,
													pmdidRel,
													iType,
													pdrgpulOld,
													pdrgpulNew
													);
	
	// project list
	CColRefSet *pcrsOutput = pexpr->Prpp()->PcrsRequired();
	CDXLNode *pdxlnPrL = NULL;
	if (NULL != pdrgpcrNew)
	{
		pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcrNew);
	}
	else
	{
		pdxlnPrL = PdxlnProjList(pcrsOutput, pdrgpcrOld);
	}

	CDXLNode *pdxlnRowTrigger = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopRowTrigger);
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexpr);
	pdxlnRowTrigger->SetProperties(pdxlprop);
	
	pdxlnRowTrigger->AddChild(pdxlnPrL);
	pdxlnRowTrigger->AddChild(pdxlnChild);

#ifdef GPOS_DEBUG
	pdxlnRowTrigger->Pdxlop()->AssertValid(pdxlnRowTrigger, false /* fValidateChildren */);
#endif
	return pdxlnRowTrigger;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Edxldmloptype
//
//	@doc:
//		Return the EdxlDmlType for a given DML op type
//
//---------------------------------------------------------------------------
EdxlDmlType
CTranslatorExprToDXL::Edxldmloptype
	(
	const CLogicalDML::EDMLOperator edmlop
	)
	const
{
	switch (edmlop)
	{
		case CLogicalDML::EdmlInsert:
			return Edxldmlinsert;

		case CLogicalDML::EdmlDelete:
			return Edxldmldelete;

		case CLogicalDML::EdmlUpdate:
			return Edxldmlupdate;

		default:
			GPOS_ASSERT(!"Unrecognized DML operation");
			return EdxldmlSentinel;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScCmp
//
//	@doc:
//		Create a DXL scalar comparison node from an optimizer scalar comparison 
//		expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScCmp
	(
	CExpression *pexprScCmp
	)
{
	GPOS_ASSERT(NULL != pexprScCmp);
	
	// extract components
	CExpression *pexprLeft = (*pexprScCmp)[0];
	CExpression *pexprRight = (*pexprScCmp)[1];

	// translate children expression
	CDXLNode *pdxlnLeft = PdxlnScalar(pexprLeft);
	CDXLNode *pdxlnRight = PdxlnScalar(pexprRight);
	
	CScalarCmp *popScCmp = CScalarCmp::PopConvert(pexprScCmp->Pop());
	
	GPOS_ASSERT(NULL != popScCmp);
	GPOS_ASSERT(NULL != popScCmp->Pstr());
	GPOS_ASSERT(NULL != popScCmp->Pstr()->Wsz());
	
	// construct a scalar comparison node
	IMDId *pmdid = popScCmp->PmdidOp();
	pmdid->AddRef();

	CWStringConst *pstrName = GPOS_NEW(m_pmp) CWStringConst(m_pmp, popScCmp->Pstr()->Wsz());
	
	CDXLNode *pdxlnCmp = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarComp(m_pmp, pmdid, pstrName));
	
	// add children
	pdxlnCmp->AddChild(pdxlnLeft);
	pdxlnCmp->AddChild(pdxlnRight);
	
#ifdef GPOS_DEBUG
	pdxlnCmp->Pdxlop()->AssertValid(pdxlnCmp, false /* fValidateChildren */);
#endif
	
	return pdxlnCmp;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScDistinctCmp
//
//	@doc:
//		Create a DXL scalar distinct comparison node from an optimizer scalar
//		is distinct from expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScDistinctCmp
	(
	CExpression *pexprScDist
	)
{
	GPOS_ASSERT(NULL != pexprScDist);

	// extract components
	CExpression *pexprLeft = (*pexprScDist)[0];
	CExpression *pexprRight = (*pexprScDist)[1];

	// translate children expression
	CDXLNode *pdxlnLeft = PdxlnScalar(pexprLeft);
	CDXLNode *pdxlnRight = PdxlnScalar(pexprRight);

	CScalarIsDistinctFrom *popScIDF = CScalarIsDistinctFrom::PopConvert(pexprScDist->Pop());

	// construct a scalar distinct comparison node
	IMDId *pmdid = popScIDF->PmdidOp();
	pmdid->AddRef();

	CDXLNode *pdxlnDistCmp = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarDistinctComp(m_pmp, pmdid));

	// add children
	pdxlnDistCmp->AddChild(pdxlnLeft);
	pdxlnDistCmp->AddChild(pdxlnRight);

#ifdef GPOS_DEBUG
	pdxlnDistCmp->Pdxlop()->AssertValid(pdxlnDistCmp, false /* fValidateChildren */);
#endif

	return pdxlnDistCmp;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScOp
//
//	@doc:
//		Create a DXL scalar op expr node from an optimizer scalar op expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScOp
	(
	CExpression *pexprOp
	)
{
	GPOS_ASSERT(NULL != pexprOp && ((1 == pexprOp->UlArity()) || (2 == pexprOp->UlArity())));
	CScalarOp *pscop = CScalarOp::PopConvert(pexprOp->Pop());

	// construct a scalar opexpr node
	CWStringConst *pstrName = GPOS_NEW(m_pmp) CWStringConst(m_pmp, pscop->Pstr()->Wsz());

	IMDId *pmdidOp = pscop->PmdidOp();
	pmdidOp->AddRef();
	
	IMDId *pmdidReturnType = pscop->PmdidReturnType();
	if (NULL != pmdidReturnType)
	{
		pmdidReturnType->AddRef();
	}
	
	CDXLNode *pdxlnOpExpr = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarOpExpr(m_pmp, pmdidOp, pmdidReturnType, pstrName));

	TranslateScalarChildren(pexprOp, pdxlnOpExpr);

#ifdef GPOS_DEBUG
	pdxlnOpExpr->Pdxlop()->AssertValid(pdxlnOpExpr, false /* fValidateChildren */);
#endif

	return pdxlnOpExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScBoolExpr
//
//	@doc:
//		Create a DXL scalar bool expression node from an optimizer scalar log op
//		expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScBoolExpr
	(
	CExpression *pexprScBoolOp
	)
{
	GPOS_ASSERT(NULL != pexprScBoolOp);
	CScalarBoolOp *popScBoolOp = CScalarBoolOp::PopConvert(pexprScBoolOp->Pop());
	EdxlBoolExprType edxlbooltype = Edxlbooltype(popScBoolOp->Eboolop());

#ifdef GPOS_DEBUG
	if(CScalarBoolOp::EboolopNot == popScBoolOp->Eboolop())
	{
		GPOS_ASSERT(1 == pexprScBoolOp->UlArity());
	}
	else
	{
		GPOS_ASSERT(2 <= pexprScBoolOp->UlArity());
	}
#endif // GPOS_DEBUG

	CDXLNode *pdxlnBoolExpr = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarBoolExpr(m_pmp, edxlbooltype));

	TranslateScalarChildren(pexprScBoolOp, pdxlnBoolExpr);

#ifdef GPOS_DEBUG
	pdxlnBoolExpr->Pdxlop()->AssertValid(pdxlnBoolExpr, false /* fValidateChildren */);
#endif

	return pdxlnBoolExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Edxlbooltype
//
//	@doc:
//		Return the EdxlBoolExprType for a given scalar logical op type
//
//---------------------------------------------------------------------------
EdxlBoolExprType
CTranslatorExprToDXL::Edxlbooltype
	(
	const CScalarBoolOp::EBoolOperator eboolop
	)
	const
{
	switch (eboolop)
	{
		case CScalarBoolOp::EboolopNot:
			return Edxlnot;

		case CScalarBoolOp::EboolopAnd:
			return Edxland;

		case CScalarBoolOp::EboolopOr:
			return Edxlor;

		default:
			GPOS_ASSERT(!"Unrecognized boolean expression type");
			return EdxlBoolExprTypeSentinel;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScId
//
//	@doc:
//		Create a DXL scalar identifier node from an optimizer scalar id expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScId
	(
	CExpression *pexprIdent
	)
{
	GPOS_ASSERT(NULL != pexprIdent);
	
	CScalarIdent *popScId = CScalarIdent::PopConvert(pexprIdent->Pop());
	CColRef *pcr = const_cast<CColRef*>(popScId->Pcr());

	return CTranslatorExprToDXLUtils::PdxlnIdent(m_pmp, m_phmcrdxln, m_phmcrdxlnIndexLookup, pcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScFuncExpr
//
//	@doc:
//		Create a DXL scalar func expr node from an optimizer scalar func expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScFuncExpr
	(
	CExpression *pexprFunc
	)
{
	GPOS_ASSERT(NULL != pexprFunc);
	
	CScalarFunc *popScFunc = CScalarFunc::PopConvert(pexprFunc->Pop());
	
	IMDId *pmdidFunc = popScFunc->PmdidFunc();
	pmdidFunc->AddRef();
	
	IMDId *pmdidRetType = popScFunc->PmdidType();
	pmdidRetType->AddRef();

	const IMDFunction *pmdfunc = m_pmda->Pmdfunc(pmdidFunc);
		
	CDXLNode *pdxlnFuncExpr = GPOS_NEW(m_pmp) CDXLNode
											(
											m_pmp,
											GPOS_NEW(m_pmp) CDXLScalarFuncExpr(m_pmp, pmdidFunc, pmdidRetType, pmdfunc->FReturnsSet())
											);
	
	// translate children
	TranslateScalarChildren(pexprFunc, pdxlnFuncExpr);
	
	return pdxlnFuncExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScWindowFuncExpr
//
//	@doc:
//		Create a DXL scalar window ref node from an optimizer scalar window
//		function expr
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScWindowFuncExpr
	(
	CExpression *pexprWindowFunc
	)
{
	GPOS_ASSERT(NULL != pexprWindowFunc);
	
	CScalarWindowFunc *popScWindowFunc = CScalarWindowFunc::PopConvert(pexprWindowFunc->Pop());
	
	IMDId *pmdidFunc = popScWindowFunc->PmdidFunc();
	pmdidFunc->AddRef();
	
	IMDId *pmdidRetType = popScWindowFunc->PmdidType();
	pmdidRetType->AddRef();

	EdxlWinStage edxlwinstage = Ews(popScWindowFunc->Ews());
	CDXLScalarWindowRef *pdxlopWindowref = GPOS_NEW(m_pmp) CDXLScalarWindowRef
															(
															m_pmp,
															pmdidFunc,
															pmdidRetType,
															popScWindowFunc->FDistinct(),
															edxlwinstage,
															0 /* ulWinspecPosition */
															);
	
	CDXLNode *pdxlnWindowRef = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopWindowref);

	// translate children
	TranslateScalarChildren(pexprWindowFunc, pdxlnWindowRef);
	
	return pdxlnWindowRef;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Ews
//
//	@doc:
//		Get the DXL representation of the window stage
//
//---------------------------------------------------------------------------
EdxlWinStage
CTranslatorExprToDXL::Ews
	(
	CScalarWindowFunc::EWinStage ews
	)
	const
{
	ULONG rgrgulMapping[][2] =
	{
		{EdxlwinstageImmediate, CScalarWindowFunc::EwsImmediate},
		{EdxlwinstagePreliminary, CScalarWindowFunc::EwsPreliminary},
		{EdxlwinstageRowKey, CScalarWindowFunc::EwsRowKey}
	};
#ifdef GPOS_DEBUG
	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	GPOS_ASSERT(ulArity > (ULONG) ews);
#endif
	ULONG *pulElem = rgrgulMapping[(ULONG) ews];
	EdxlWinStage edxlws = (EdxlWinStage) pulElem[0];
	
	return edxlws;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScAggref
//
//	@doc:
//		Create a DXL scalar aggref node from an optimizer scalar agg func expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScAggref
	(
	CExpression *pexprAggFunc
	)
{
	GPOS_ASSERT(NULL != pexprAggFunc);
	
	CScalarAggFunc *popScAggFunc = CScalarAggFunc::PopConvert(pexprAggFunc->Pop());
	IMDId *pmdidAggFunc = popScAggFunc->Pmdid();
	pmdidAggFunc->AddRef();
	
	IMDId *pmdidResolvedRetType = NULL;
	if (popScAggFunc->FHasAmbiguousReturnType())
	{
		// Agg has an ambiguous return type, use the resolved type instead
		pmdidResolvedRetType = popScAggFunc->PmdidType();
		pmdidResolvedRetType->AddRef();
	}

	EdxlAggrefStage edxlaggrefstage = EdxlaggstageNormal;

	if (popScAggFunc->FGlobal() && popScAggFunc->FSplit())
	{
		edxlaggrefstage = EdxlaggstageFinal;
	}
	else if (EaggfuncstageIntermediate == popScAggFunc->Eaggfuncstage())
	{
		edxlaggrefstage = EdxlaggstageIntermediate;
	}
	else if (!popScAggFunc->FGlobal())
	{
		edxlaggrefstage = EdxlaggstagePartial;
	}

	CDXLScalarAggref *pdxlopAggRef = GPOS_NEW(m_pmp) CDXLScalarAggref
												(
												m_pmp,
												pmdidAggFunc,
												pmdidResolvedRetType,
												popScAggFunc->FDistinct(),
												edxlaggrefstage
												);

	CDXLNode *pdxlnAggref = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopAggRef);
	TranslateScalarChildren(pexprAggFunc, pdxlnAggref);
	
	return pdxlnAggref;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScIfStmt
//
//	@doc:
//		Create a DXL scalar if node from an optimizer scalar if expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScIfStmt
	(
	CExpression *pexprIfStmt
	)
{
	GPOS_ASSERT(NULL != pexprIfStmt);

	GPOS_ASSERT(3 == pexprIfStmt->UlArity());

	CScalarIf *popScIf = CScalarIf::PopConvert(pexprIfStmt->Pop());

	IMDId *pmdidType = popScIf->PmdidType();
	pmdidType->AddRef();
	
	CDXLNode *pdxlnIfStmt = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarIfStmt(m_pmp, pmdidType));
	TranslateScalarChildren(pexprIfStmt, pdxlnIfStmt);

	return pdxlnIfStmt;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScSwitch
//
//	@doc:
//		Create a DXL scalar switch node from an optimizer scalar switch expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScSwitch
	(
	CExpression *pexprSwitch
	)
{
	GPOS_ASSERT(NULL != pexprSwitch);
	GPOS_ASSERT(1 < pexprSwitch->UlArity());
	CScalarSwitch *pop = CScalarSwitch::PopConvert(pexprSwitch->Pop());

	IMDId *pmdidType = pop->PmdidType();
	pmdidType->AddRef();

	CDXLNode *pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarSwitch(m_pmp, pmdidType));
	TranslateScalarChildren(pexprSwitch, pdxln);

	return pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScSwitchCase
//
//	@doc:
//		Create a DXL scalar switch case node from an optimizer scalar switch
//		case expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScSwitchCase
	(
	CExpression *pexprSwitchCase
	)
{
	GPOS_ASSERT(NULL != pexprSwitchCase);
	GPOS_ASSERT(2 == pexprSwitchCase->UlArity());

	CDXLNode *pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarSwitchCase(m_pmp));
	TranslateScalarChildren(pexprSwitchCase, pdxln);

	return pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScNullIf
//
//	@doc:
//		Create a DXL scalar nullif node from an optimizer scalar
//		nullif expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScNullIf
	(
	CExpression *pexprScNullIf
	)
{
	GPOS_ASSERT(NULL != pexprScNullIf);

	CScalarNullIf *pop = CScalarNullIf::PopConvert(pexprScNullIf->Pop());

	IMDId *pmdid = pop->PmdidOp();
	pmdid->AddRef();

	IMDId *pmdidType = pop->PmdidType();
	pmdidType->AddRef();

	CDXLScalarNullIf *pdxlop = GPOS_NEW(m_pmp) CDXLScalarNullIf(m_pmp, pmdid, pmdidType);
	CDXLNode *pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	TranslateScalarChildren(pexprScNullIf, pdxln);

	return pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScCaseTest
//
//	@doc:
//		Create a DXL scalar case test node from an optimizer scalar case test
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScCaseTest
	(
	CExpression *pexprScCaseTest
	)
{
	GPOS_ASSERT(NULL != pexprScCaseTest);
	CScalarCaseTest *pop = CScalarCaseTest::PopConvert(pexprScCaseTest->Pop());

	IMDId *pmdidType = pop->PmdidType();
	pmdidType->AddRef();

	CDXLScalarCaseTest *pdxlop = GPOS_NEW(m_pmp) CDXLScalarCaseTest(m_pmp, pmdidType);

	return GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScNullTest
//
//	@doc:
//		Create a DXL scalar null test node from an optimizer scalar null test expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScNullTest
	(
	CExpression *pexprNullTest
	)
{
	GPOS_ASSERT(NULL != pexprNullTest);
	
	CDXLNode *pdxlnNullTest = GPOS_NEW(m_pmp) CDXLNode
											(
											m_pmp, 
											GPOS_NEW(m_pmp) CDXLScalarNullTest(m_pmp, true /* fIsNull */));
	
	// translate child
	GPOS_ASSERT(1 == pexprNullTest->UlArity());
	
	CExpression *pexprChild = (*pexprNullTest)[0];
	CDXLNode *pdxlnChild = PdxlnScalar(pexprChild);
	pdxlnNullTest->AddChild(pdxlnChild);
	
	return pdxlnNullTest;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScBooleanTest
//
//	@doc:
//		Create a DXL scalar null test node from an optimizer scalar null test expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScBooleanTest
	(
	CExpression *pexprScBooleanTest
	)
{
	GPOS_ASSERT(NULL != pexprScBooleanTest);
	GPOS_ASSERT(1 == pexprScBooleanTest->UlArity());

	const ULONG rgulBoolTestMapping[][2] =
	{
		{CScalarBooleanTest::EbtIsTrue, EdxlbooleantestIsTrue},
		{CScalarBooleanTest::EbtIsNotTrue, EdxlbooleantestIsNotTrue},
		{CScalarBooleanTest::EbtIsFalse, EdxlbooleantestIsFalse},
		{CScalarBooleanTest::EbtIsNotFalse, EdxlbooleantestIsNotFalse},
		{CScalarBooleanTest::EbtIsUnknown, EdxlbooleantestIsUnknown},
		{CScalarBooleanTest::EbtIsNotUnknown, EdxlbooleantestIsNotUnknown},
	};

	CScalarBooleanTest *popBoolTest = CScalarBooleanTest::PopConvert(pexprScBooleanTest->Pop());
	EdxlBooleanTestType edxlbooltest = (EdxlBooleanTestType) (rgulBoolTestMapping[popBoolTest->Ebt()][1]);
	CDXLNode *pdxlnScBooleanTest = GPOS_NEW(m_pmp) CDXLNode
											(
											m_pmp,
											GPOS_NEW(m_pmp) CDXLScalarBooleanTest(m_pmp, edxlbooltest)
											);

	// translate child
	CExpression *pexprChild = (*pexprScBooleanTest)[0];
	CDXLNode *pdxlnChild = PdxlnScalar(pexprChild);
	pdxlnScBooleanTest->AddChild(pdxlnChild);

	return pdxlnScBooleanTest;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScCoalesce
//
//	@doc:
//		Create a DXL scalar coalesce node from an optimizer scalar coalesce expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScCoalesce
	(
	CExpression *pexprCoalesce
	)
{
	GPOS_ASSERT(NULL != pexprCoalesce);
	GPOS_ASSERT(0 < pexprCoalesce->UlArity());
	CScalarCoalesce *popScCoalesce = CScalarCoalesce::PopConvert(pexprCoalesce->Pop());

	IMDId *pmdidType = popScCoalesce->PmdidType();
	pmdidType->AddRef();

	CDXLNode *pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarCoalesce(m_pmp, pmdidType));
	TranslateScalarChildren(pexprCoalesce, pdxln);

	return pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScMinMax
//
//	@doc:
//		Create a DXL scalar MinMax node from an optimizer scalar MinMax expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScMinMax
	(
	CExpression *pexprMinMax
	)
{
	GPOS_ASSERT(NULL != pexprMinMax);
	GPOS_ASSERT(0 < pexprMinMax->UlArity());
	CScalarMinMax *popScMinMax = CScalarMinMax::PopConvert(pexprMinMax->Pop());

	CScalarMinMax::EScalarMinMaxType esmmt = popScMinMax->Esmmt();
	GPOS_ASSERT(CScalarMinMax::EsmmtMin == esmmt || CScalarMinMax::EsmmtMax == esmmt);

	CDXLScalarMinMax::EdxlMinMaxType emmt = CDXLScalarMinMax::EmmtMin;
	if (CScalarMinMax::EsmmtMax == esmmt)
	{
		emmt = CDXLScalarMinMax::EmmtMax;
	}

	IMDId *pmdidType = popScMinMax->PmdidType();
	pmdidType->AddRef();

	CDXLNode *pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarMinMax(m_pmp, pmdidType, emmt));
	TranslateScalarChildren(pexprMinMax, pdxln);

	return pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::TranslateScalarChildren
//
//	@doc:
//		Translate expression children and add them as children of the DXL node
//
//---------------------------------------------------------------------------
void
CTranslatorExprToDXL::TranslateScalarChildren
	(
	CExpression *pexpr,
	CDXLNode *pdxln
	)
{
	const ULONG ulArity = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		CDXLNode *pdxlnChild = PdxlnScalar(pexprChild);
		pdxln->AddChild(pdxlnChild);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScCast
//
//	@doc:
//		Create a DXL scalar relabel type node from an
//		optimizer scalar relabel type expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScCast
	(
	CExpression *pexprCast
	)
{
	GPOS_ASSERT(NULL != pexprCast);
	CScalarCast *popScCast = CScalarCast::PopConvert(pexprCast->Pop());

	IMDId *pmdid = popScCast->PmdidType();
	pmdid->AddRef();

	IMDId *pmdidFunc = popScCast->PmdidFunc();
	pmdidFunc->AddRef();

	CDXLNode *pdxlnCast = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarCast(m_pmp, pmdid, pmdidFunc));

	// translate child
	GPOS_ASSERT(1 == pexprCast->UlArity());
	CExpression *pexprChild = (*pexprCast)[0];
	CDXLNode *pdxlnChild = PdxlnScalar(pexprChild);
	pdxlnCast->AddChild(pdxlnChild);

	return pdxlnCast;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScCoerceToDomain
//
//	@doc:
//		Create a DXL scalar coerce node from an optimizer scalar coerce expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScCoerceToDomain
	(
	CExpression *pexprCoerce
	)
{
	GPOS_ASSERT(NULL != pexprCoerce);
	CScalarCoerceToDomain *popScCerce = CScalarCoerceToDomain::PopConvert(pexprCoerce->Pop());

	IMDId *pmdid = popScCerce->PmdidType();
	pmdid->AddRef();


	CDXLNode *pdxlnCoerce =
		GPOS_NEW(m_pmp) CDXLNode
			(
			m_pmp,
			GPOS_NEW(m_pmp) CDXLScalarCoerceToDomain
					(
					m_pmp,
					pmdid,
					popScCerce->IMod(),
					(EdxlCoercionForm) popScCerce->Ecf(), // map Coercion Form directly based on position in enum
					popScCerce->ILoc()
					)
			);

	// translate child
	GPOS_ASSERT(1 == pexprCoerce->UlArity());
	CExpression *pexprChild = (*pexprCoerce)[0];
	CDXLNode *pdxlnChild = PdxlnScalar(pexprChild);
	pdxlnCoerce->AddChild(pdxlnChild);

	return pdxlnCoerce;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScCoerceViaIO
//
//	@doc:
//		Create a DXL scalar coerce node from an optimizer scalar coerce expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScCoerceViaIO
	(
	CExpression *pexprCoerce
	)
{
	GPOS_ASSERT(NULL != pexprCoerce);
	CScalarCoerceViaIO *popScCerce = CScalarCoerceViaIO::PopConvert(pexprCoerce->Pop());

	IMDId *pmdid = popScCerce->PmdidType();
	pmdid->AddRef();


	CDXLNode *pdxlnCoerce =
		GPOS_NEW(m_pmp) CDXLNode
			(
			m_pmp,
			GPOS_NEW(m_pmp) CDXLScalarCoerceViaIO
					(
					m_pmp,
					pmdid,
					popScCerce->IMod(),
					(EdxlCoercionForm) popScCerce->Ecf(), // map Coercion Form directly based on position in enum
					popScCerce->ILoc()
					)
			);

	// translate child
	GPOS_ASSERT(1 == pexprCoerce->UlArity());
	CExpression *pexprChild = (*pexprCoerce)[0];
	CDXLNode *pdxlnChild = PdxlnScalar(pexprChild);
	pdxlnCoerce->AddChild(pdxlnChild);

	return pdxlnCoerce;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Pdxlwf
//
//	@doc:
//		Translate a window frame
//
//---------------------------------------------------------------------------
CDXLWindowFrame *
CTranslatorExprToDXL::Pdxlwf
	(
	CWindowFrame *pwf
	)
{
	GPOS_ASSERT(NULL != pwf);

	if (CWindowFrame::FEmpty(pwf))
	{
		// an empty frame is translated as 'no frame'
		return NULL;
	}
		
	// mappings for frame info in expression and dxl worlds
	const ULONG rgulSpecMapping[][2] =
	{
		{CWindowFrame::EfsRows, EdxlfsRow},
		{CWindowFrame::EfsRange, EdxlfsRange}
	};

	const ULONG rgulBoundaryMapping[][2] =
	{
		{CWindowFrame::EfbUnboundedPreceding, EdxlfbUnboundedPreceding},
		{CWindowFrame::EfbBoundedPreceding, EdxlfbBoundedPreceding},
		{CWindowFrame::EfbCurrentRow, EdxlfbCurrentRow},
		{CWindowFrame::EfbUnboundedFollowing, EdxlfbUnboundedFollowing},
		{CWindowFrame::EfbBoundedFollowing, EdxlfbBoundedFollowing},
		{CWindowFrame::EfbDelayedBoundedPreceding, EdxlfbDelayedBoundedPreceding},
		{CWindowFrame::EfbDelayedBoundedFollowing, EdxlfbDelayedBoundedFollowing}
	};

	const ULONG rgulExclusionStrategyMapping[][2] =
	{
		{CWindowFrame::EfesNone, EdxlfesNone},
		{CWindowFrame::EfesNulls, EdxlfesNulls},
		{CWindowFrame::EfesCurrentRow, EdxlfesCurrentRow},
		{CWindowFrame::EfseMatchingOthers, EdxlfesGroup},
		{CWindowFrame::EfesTies, EdxlfesTies}
	};

	EdxlFrameSpec edxlfs = (EdxlFrameSpec) (rgulSpecMapping[pwf->Efs()][1]);
	EdxlFrameBoundary edxlfbLeading = (EdxlFrameBoundary) (rgulBoundaryMapping[pwf->EfbLeading()][1]);
	EdxlFrameBoundary edxlfbTrailing = (EdxlFrameBoundary) (rgulBoundaryMapping[pwf->EfbTrailing()][1]);
	EdxlFrameExclusionStrategy edxlfes = (EdxlFrameExclusionStrategy) (rgulExclusionStrategyMapping[pwf->Efes()][1]);

	// translate scalar expressions representing leading and trailing frame edges
	CDXLNode *pdxlnLeading = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarWindowFrameEdge(m_pmp, true /* fLeading */, edxlfbLeading));
	if (NULL != pwf->PexprLeading())
	{
		pdxlnLeading->AddChild(PdxlnScalar(pwf->PexprLeading()));
	}

	CDXLNode *pdxlnTrailing = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarWindowFrameEdge(m_pmp, false /* fLeading */, edxlfbTrailing));
	if (NULL != pwf->PexprTrailing())
	{
		pdxlnTrailing->AddChild(PdxlnScalar(pwf->PexprTrailing()));
	}

	return GPOS_NEW(m_pmp) CDXLWindowFrame(m_pmp, edxlfs, edxlfes, pdxlnLeading, pdxlnTrailing);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnWindow
//
//	@doc:
//		Create a DXL window node from physical sequence project expression.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnWindow
	(
	CExpression *pexprSeqPrj,
	DrgPcr *pdrgpcr,
	DrgPds *pdrgpdsBaseTables, 
	ULONG *pulNonGatherMotions,
	BOOL *pfDML
	)
{
	GPOS_ASSERT(NULL != pexprSeqPrj);

	CPhysicalSequenceProject *popSeqPrj = CPhysicalSequenceProject::PopConvert(pexprSeqPrj->Pop());
	CDistributionSpec *pds = popSeqPrj->Pds();
	DrgPul *pdrgpulColIds = GPOS_NEW(m_pmp) DrgPul(m_pmp);
	DrgPexpr *pdrgpexprPartCol = NULL;
	if (CDistributionSpec::EdtHashed == pds->Edt())
	{
		CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);
		pdrgpexprPartCol = const_cast<DrgPexpr *>(pdshashed->Pdrgpexpr());
		const ULONG ulSize = pdrgpexprPartCol->UlLength();
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			CExpression *pexpr = (*pdrgpexprPartCol)[ul];
			CScalarIdent *popScId = CScalarIdent::PopConvert(pexpr->Pop());
			pdrgpulColIds->Append(GPOS_NEW(m_pmp) ULONG(popScId->Pcr()->UlId()));
		}
	}

	// translate order specification and window frames into window keys
	DrgPdxlwk *pdrgpdxlwk = GPOS_NEW(m_pmp) DrgPdxlwk(m_pmp);
	DrgPos *pdrgpos = popSeqPrj->Pdrgpos();
	GPOS_ASSERT(NULL != pdrgpos);
	const ULONG ulOsSize = pdrgpos->UlLength();
	for (ULONG ul = 0; ul < ulOsSize; ul++)
	{
		CDXLWindowKey *pdxlwk = GPOS_NEW(m_pmp) CDXLWindowKey(m_pmp);
		CDXLNode *pdxlnSortColList = PdxlnSortColList((*popSeqPrj->Pdrgpos())[ul]);
		pdxlwk->SetSortColList(pdxlnSortColList);
		pdrgpdxlwk->Append(pdxlwk);
	}

	const ULONG ulFrames = popSeqPrj->Pdrgpwf()->UlLength();
	for (ULONG ul = 0; ul < ulFrames; ul++)
	{
		CDXLWindowFrame *pdxlwf = Pdxlwf((*popSeqPrj->Pdrgpwf())[ul]);
		if (NULL != pdxlwf)
		{
			GPOS_ASSERT(ul <= ulOsSize);
			CDXLWindowKey *pdxlwk = (*pdrgpdxlwk)[ul];
			pdxlwk->SetWindowFrame(pdxlwf);
		}
	}

	// extract physical properties
	CDXLPhysicalProperties *pdxlprop = Pdxlprop(pexprSeqPrj);

	// translate relational child
	CDXLNode *pdxlnChild = Pdxln((*pexprSeqPrj)[0], NULL /* pdrgpcr */, pdrgpdsBaseTables, pulNonGatherMotions, pfDML, false /*fRemap*/, false /*fRoot*/);

	GPOS_ASSERT(NULL != pexprSeqPrj->Prpp());
	CColRefSet *pcrsOutput = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrsOutput->Include(pexprSeqPrj->Prpp()->PcrsRequired());
	if (NULL != pdrgpexprPartCol)
	{
		CColRefSet *pcrs = CUtils::PcrsExtractColumns(m_pmp, pdrgpexprPartCol);
		pcrsOutput->Include(pcrs);
		pcrs->Release();
	}
	for (ULONG ul = 0; ul < ulOsSize; ul++)
	{
		COrderSpec *pos = (*popSeqPrj->Pdrgpos())[ul];
		if (!pos->FEmpty())
		{
			const CColRef *pcr = pos->Pcr(ul);
			pcrsOutput->Include(pcr);
		}
	}
	
	// translate project list expression
	CDXLNode *pdxlnPrL = PdxlnProjList((*pexprSeqPrj)[1], pcrsOutput, pdrgpcr);

	// create an empty one-time filter
	CDXLNode *pdxlnFilter = PdxlnFilter(NULL /* pdxlnCond */);

	// construct a Window node	
	CDXLPhysicalWindow *pdxlopWindow = GPOS_NEW(m_pmp) CDXLPhysicalWindow(m_pmp, pdrgpulColIds, pdrgpdxlwk);
	CDXLNode *pdxlnWindow = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopWindow);
	pdxlnWindow->SetProperties(pdxlprop);
	
	// add children
	pdxlnWindow->AddChild(pdxlnPrL);
	pdxlnWindow->AddChild(pdxlnFilter);
	pdxlnWindow->AddChild(pdxlnChild);

#ifdef GPOS_DEBUG
	pdxlopWindow->AssertValid(pdxlnWindow, false /* fValidateChildren */);
#endif

	pcrsOutput->Release();

	return pdxlnWindow;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnArray
//
//	@doc:
//		Create a DXL array node from an optimizer array expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnArray
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CScalarArray *pop = CScalarArray::PopConvert(pexpr->Pop());

	IMDId *pmdidElem = pop->PmdidElem();
	pmdidElem->AddRef();
	
	IMDId *pmdidArray = pop->PmdidArray();
	pmdidArray->AddRef();

	CDXLNode *pdxlnArray = 
			GPOS_NEW(m_pmp) CDXLNode
						(
						m_pmp,
						GPOS_NEW(m_pmp) CDXLScalarArray
									(
									m_pmp, 
									pmdidElem,
									pmdidArray,
									pop->FMultiDimensional()
									)
						);

	TranslateScalarChildren(pexpr, pdxlnArray);
	
	return pdxlnArray;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnArrayRef
//
//	@doc:
//		Create a DXL arrayref node from an optimizer arrayref expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnArrayRef
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CScalarArrayRef *pop = CScalarArrayRef::PopConvert(pexpr->Pop());

	IMDId *pmdidElem = pop->PmdidElem();
	pmdidElem->AddRef();

	IMDId *pmdidArray = pop->PmdidArray();
	pmdidArray->AddRef();

	IMDId *pmdidReturn = pop->PmdidType();
	pmdidReturn->AddRef();

	CDXLNode *pdxlnArrayref =
			GPOS_NEW(m_pmp) CDXLNode
						(
						m_pmp,
						GPOS_NEW(m_pmp) CDXLScalarArrayRef
									(
									m_pmp,
									pmdidElem,
									pmdidArray,
									pmdidReturn
									)
						);

	TranslateScalarChildren(pexpr, pdxlnArrayref);

	return pdxlnArrayref;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnArrayRefIndexList
//
//	@doc:
//		Create a DXL arrayref index list from an optimizer arrayref index list
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnArrayRefIndexList
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CScalarArrayRefIndexList *pop = CScalarArrayRefIndexList::PopConvert(pexpr->Pop());

	CDXLNode *pdxlnIndexlist =
			GPOS_NEW(m_pmp) CDXLNode
						(
						m_pmp,
						GPOS_NEW(m_pmp) CDXLScalarArrayRefIndexList
									(
									m_pmp,
									Eilb(pop->Eilt())
									)
						);

	TranslateScalarChildren(pexpr, pdxlnIndexlist);

	return pdxlnIndexlist;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnAssertPredicate
//
//	@doc:
//		Create a DXL assert predicate from an optimizer assert predicate expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnAssertPredicate
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	CDXLNode *pdxlnAssertConstraintList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarAssertConstraintList(m_pmp));
	TranslateScalarChildren(pexpr, pdxlnAssertConstraintList);
	return pdxlnAssertConstraintList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnAssertConstraint
//
//	@doc:
//		Create a DXL assert constraint from an optimizer assert constraint expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnAssertConstraint
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CScalarAssertConstraint *popAssertConstraint = CScalarAssertConstraint::PopConvert(pexpr->Pop());
	CWStringDynamic *pstrErrorMsg = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp, popAssertConstraint->PstrErrorMsg()->Wsz());
	
	CDXLNode *pdxlnAssertConstraint  = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarAssertConstraint(m_pmp, pstrErrorMsg));
	TranslateScalarChildren(pexpr, pdxlnAssertConstraint);
	return pdxlnAssertConstraint;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Eilb
//
//	@doc:
// 		Translate the arrayref index list bound
//
//---------------------------------------------------------------------------
CDXLScalarArrayRefIndexList::EIndexListBound
CTranslatorExprToDXL::Eilb
	(
	const CScalarArrayRefIndexList::EIndexListType eilt
	)
{
	switch (eilt)
	{
		case CScalarArrayRefIndexList::EiltLower:
			return CDXLScalarArrayRefIndexList::EilbLower;

		case CScalarArrayRefIndexList::EiltUpper:
			return CDXLScalarArrayRefIndexList::EilbUpper;

		default:
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, GPOS_WSZ_LIT("Invalid arrayref index bound"));
			return CDXLScalarArrayRefIndexList::EilbSentinel;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnArrayCmp
//
//	@doc:
//		Create a DXL array compare node from an optimizer array expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnArrayCmp
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CScalarArrayCmp *pop = CScalarArrayCmp::PopConvert(pexpr->Pop());

	IMDId *pmdidOp = pop->PmdidOp();
	pmdidOp->AddRef();

	const CWStringConst *pstrOpName = pop->Pstr();
	
	CScalarArrayCmp::EArrCmpType earrcmpt = pop->Earrcmpt();
	GPOS_ASSERT(CScalarArrayCmp::EarrcmpSentinel > earrcmpt);
	EdxlArrayCompType edxlarrcmpt = Edxlarraycomptypeall;
	if (CScalarArrayCmp::EarrcmpAny == earrcmpt)
	{
		edxlarrcmpt = Edxlarraycomptypeany; 
	}
	
	CDXLNode *pdxlnArrayCmp = 
			GPOS_NEW(m_pmp) CDXLNode
						(
						m_pmp,
						GPOS_NEW(m_pmp) CDXLScalarArrayComp
									(
									m_pmp, 
									pmdidOp,
									GPOS_NEW(m_pmp) CWStringConst(m_pmp, pstrOpName->Wsz()),
									edxlarrcmpt
									)
						);

	TranslateScalarChildren(pexpr, pdxlnArrayCmp);
	
	return pdxlnArrayCmp;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnDMLAction
//
//	@doc:
//		Create a DXL DML action node from an optimizer action expression
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnDMLAction
	(
	CExpression *
#ifdef GPOS_DEBUG
	pexpr
#endif // GPOS_DEBUG
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopScalarDMLAction == pexpr->Pop()->Eopid());

	return GPOS_NEW(m_pmp) CDXLNode
						(
						m_pmp,
						GPOS_NEW(m_pmp) CDXLScalarDMLAction(m_pmp)
						);

}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnScConst
//
//	@doc:
//		Create a DXL scalar constant node from an optimizer scalar const expr.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnScConst
	(
	CExpression *pexprScConst
	)
{
	GPOS_ASSERT(NULL != pexprScConst);
	
	CScalarConst *popScConst = CScalarConst::PopConvert(pexprScConst->Pop());

	IDatum *pdatum = popScConst->Pdatum();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDType *pmdtype = pmda->Pmdtype(pdatum->Pmdid());

	CDXLNode *pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pmdtype->PdxlopScConst(m_pmp, pdatum));
	
	return pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnFilter
//
//	@doc:
//		Create a DXL filter node containing the given scalar node as a child.
//		If the scalar node is NULL, a filter node with no children is returned
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnFilter
	(
	CDXLNode *pdxlnCond
	)
{	
	CDXLNode *pdxlnFilter = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarFilter(m_pmp));
	if (NULL != pdxlnCond)
	{
		pdxlnFilter->AddChild(pdxlnCond);
	}
	
	return pdxlnFilter;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Pdxltabdesc
//
//	@doc:
//		Create a DXL table descriptor from the corresponding optimizer structure
//
//---------------------------------------------------------------------------
CDXLTableDescr *
CTranslatorExprToDXL::Pdxltabdesc
	(
	const CTableDescriptor *ptabdesc,
	const DrgPcr *pdrgpcrOutput
	)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT_IMP(NULL != pdrgpcrOutput, ptabdesc->UlColumns() == pdrgpcrOutput->UlLength());
	
	// get tbl name
	CMDName *pmdnameTbl = GPOS_NEW(m_pmp) CMDName(m_pmp, ptabdesc->Name().Pstr());
	
	CMDIdGPDB *pmdid = CMDIdGPDB::PmdidConvert(ptabdesc->Pmdid());
	pmdid->AddRef();
	
	CDXLTableDescr *pdxltabdesc = GPOS_NEW(m_pmp) CDXLTableDescr(m_pmp, pmdid, pmdnameTbl, ptabdesc->UlExecuteAsUser());

	const ULONG ulColumns = ptabdesc->UlColumns();
	// translate col descriptors
	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		const CColumnDescriptor *pcd = ptabdesc->Pcoldesc(ul);
		
		GPOS_ASSERT(NULL != pcd);
		
		CMDName *pmdnameCol = GPOS_NEW(m_pmp) CMDName(m_pmp, pcd->Name().Pstr());
		 
		// output col ref for the current col descrs
		CColRef *pcr = NULL;
		if (NULL != pdrgpcrOutput)
		{
			pcr = (*pdrgpcrOutput)[ul];
		}
		else
		{
			pcr = m_pcf->PcrCreate(pcd->Pmdtype(), pcd->Name());
		}

		// use the col ref id for the corresponding output output column as 
		// colid for the dxl column
		CMDIdGPDB *pmdidColType = CMDIdGPDB::PmdidConvert(pcr->Pmdtype()->Pmdid());
		pmdidColType->AddRef();
		
		CDXLColDescr *pdxlcd = GPOS_NEW(m_pmp) CDXLColDescr
											(
											m_pmp,
											pmdnameCol,
											pcr->UlId(),
											pcd->IAttno(),
											pmdidColType,
											false /* fdropped */,
											pcd->UlWidth()
											);
		
		pdxltabdesc->AddColumnDescr(pdxlcd);		
	}
	
	return pdxltabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::Pdxlprop
//
//	@doc:
//		Construct a DXL physical properties container with operator costs for
//		the given expression
//
//---------------------------------------------------------------------------
CDXLPhysicalProperties *
CTranslatorExprToDXL::Pdxlprop
	(
	const CExpression *pexpr
	)
{

	// extract out rows from statistics object
	CWStringDynamic *pstrRows = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp);
	const IStatistics *pstats = pexpr->Pstats();
	CDouble dRows = CStatistics::DDefaultRelationRows;

	// stats may not be present in artificially generated physical expression trees.
	// fill in default statistics
	if (NULL != pstats)
	{
		dRows = pstats->DRows();
	}

	if (CDistributionSpec::EdtReplicated == CDrvdPropPlan::Pdpplan(pexpr->Pdp(CDrvdProp::EptPlan))->Pds()->Edt())
	{
		// if distribution is replicated, multiply number of rows by number of segments
		ULONG ulSegments = COptCtxt::PoctxtFromTLS()->Pcm()->UlHosts();
		dRows = dRows * ulSegments;
	}

	pstrRows->AppendFormat(GPOS_WSZ_LIT("%f"), dRows.DVal());

	// extract our width from statistics object
	CDouble dWidth = CStatistics::DDefaultColumnWidth;
	CReqdPropPlan *prpp = pexpr->Prpp();
	CColRefSet *pcrs = prpp->PcrsRequired();
	DrgPul *pdrgpulColIds = GPOS_NEW(m_pmp) DrgPul(m_pmp);
	pcrs->ExtractColIds(m_pmp, pdrgpulColIds);
	CWStringDynamic *pstrWidth = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp);

	if (NULL != pstats)
	{
		dWidth = pstats->DWidth(pdrgpulColIds);
	}
	pdrgpulColIds->Release();
	pstrWidth->AppendFormat(GPOS_WSZ_LIT("%lld"), (LINT) dWidth.DVal());

	// get the cost from expression node
	CWStringDynamic str(m_pmp);
	COstreamString oss (&str);
	oss << pexpr->Cost();

	CWStringDynamic *pstrStartupcost = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp, GPOS_WSZ_LIT("0"));
	CWStringDynamic *pstrTotalcost = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp, str.Wsz());

	CDXLOperatorCost *pdxlopcost = GPOS_NEW(m_pmp) CDXLOperatorCost(pstrStartupcost, pstrTotalcost, pstrRows, pstrWidth);
	CDXLPhysicalProperties *pdxlprop = GPOS_NEW(m_pmp) CDXLPhysicalProperties(pdxlopcost);

	return pdxlprop;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnProjList
//
//	@doc:
//		Translate the set of output col refs into a dxl project list.
//		If the given array of columns is not NULL, it specifies the order of the 
//		columns in the project list, otherwise any order is good
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnProjList
	(
	const CColRefSet *pcrsOutput,
	DrgPcr *pdrgpcr
	)
{
	GPOS_ASSERT(NULL != pcrsOutput);
	
	CDXLScalarProjList *pdxlopPrL = GPOS_NEW(m_pmp) CDXLScalarProjList(m_pmp);
	CDXLNode *pdxlnPrL = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopPrL);

	if (NULL != pdrgpcr)
	{
		CColRefSet *pcrs= GPOS_NEW(m_pmp) CColRefSet(m_pmp);
		
		for (ULONG ul = 0; ul < pdrgpcr->UlLength(); ul++)
		{
			CColRef *pcr = (*pdrgpcr)[ul];

			CDXLNode *pdxlnPrEl = CTranslatorExprToDXLUtils::PdxlnProjElem(m_pmp, m_phmcrdxln, pcr);
			pdxlnPrL->AddChild(pdxlnPrEl);
			pcrs->Include(pcr);
		}

		// add the remaining required columns
		CColRefSetIter crsi(*pcrsOutput);
		while(crsi.FAdvance())
		{
			CColRef *pcr = crsi.Pcr();

			if (!pcrs->FMember(pcr))
			{
				CDXLNode *pdxlnPrEl = CTranslatorExprToDXLUtils::PdxlnProjElem(m_pmp, m_phmcrdxln, pcr);
				pdxlnPrL->AddChild(pdxlnPrEl);
				pcrs->Include(pcr);
			}
		}
		pcrs->Release();
	}
	else
	{
		// no order specified
		CColRefSetIter crsi(*pcrsOutput);
		while(crsi.FAdvance())
		{
			CColRef *pcr = crsi.Pcr();
			CDXLNode *pdxlnPrEl = CTranslatorExprToDXLUtils::PdxlnProjElem(m_pmp, m_phmcrdxln, pcr);
			pdxlnPrL->AddChild(pdxlnPrEl);
		}
	}
	
	return pdxlnPrL;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnProjList
//
//	@doc:
//		 Translate a project list expression into DXL project list node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnProjList
	(
	const CExpression *pexprProjList,
	const CColRefSet *pcrsRequired,
	DrgPcr *pdrgpcr
	)
{
	if (NULL == pdrgpcr)
	{
		// no order specified
		return PdxlnProjList(pexprProjList, pcrsRequired);
	}

	// translate computed column expressions into DXL and index them on their col ids
	CHashMap<ULONG, CDXLNode, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>, CleanupDelete<ULONG>, CleanupRelease<CDXLNode> >
		*phmComputedColumns = GPOS_NEW(m_pmp) CHashMap<ULONG, CDXLNode, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>, CleanupDelete<ULONG>, CleanupRelease<CDXLNode> >(m_pmp);
	
	for (ULONG ul = 0; NULL != pexprProjList && ul < pexprProjList->UlArity(); ul++)
	{
		CExpression *pexprProjElem = (*pexprProjList)[ul];

		// translate proj elem
		CDXLNode *pdxlnProjElem = PdxlnProjElem(pexprProjElem);
		
		const CScalarProjectElement *popScPrEl =
				CScalarProjectElement::PopConvert(pexprProjElem->Pop());
		
		ULONG *pulKey = GPOS_NEW(m_pmp) ULONG(popScPrEl->Pcr()->UlId());
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif // GPOS_DEBUG
		phmComputedColumns->FInsert(pulKey, pdxlnProjElem);
		
		GPOS_ASSERT(fInserted);
	}

	// add required columns to the project list
	DrgPcr *pdrgpcrCopy = GPOS_NEW(m_pmp) DrgPcr(m_pmp);
	pdrgpcrCopy->AppendArray(pdrgpcr);
	CColRefSet *pcrsOutput = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrsOutput->Include(pdrgpcr);
	CColRefSetIter crsi(*pcrsRequired);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		if (!pcrsOutput->FMember(pcr))
		{
			pdrgpcrCopy->Append(pcr);
		}
	}
	
	// translate project list according to the specified order
	CDXLScalarProjList *pdxlopPrL = GPOS_NEW(m_pmp) CDXLScalarProjList(m_pmp);
	CDXLNode *pdxlnProjList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopPrL);

	const ULONG ulCols = pdrgpcrCopy->UlLength();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColRef *pcr = (*pdrgpcrCopy)[ul];
		ULONG ulKey = pcr->UlId();
		CDXLNode *pdxlnProjElem = phmComputedColumns->PtLookup(&ulKey);
		
		if (NULL == pdxlnProjElem)
		{
			// not a computed column
			pdxlnProjElem = CTranslatorExprToDXLUtils::PdxlnProjElem(m_pmp, m_phmcrdxln, pcr);
		}
		else
		{
			pdxlnProjElem->AddRef();
		}
		
		pdxlnProjList->AddChild(pdxlnProjElem);
	}

	// cleanup
	pdrgpcrCopy->Release();
	pcrsOutput->Release();
	phmComputedColumns->Release();
	
	return pdxlnProjList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnProjList
//
//	@doc:
//		 Translate a project list expression into DXL project list node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnProjList
	(
	const CExpression *pexprProjList,
	const CColRefSet *pcrsRequired
	)
{
	CDXLScalarProjList *pdxlopPrL = GPOS_NEW(m_pmp) CDXLScalarProjList(m_pmp);
	CDXLNode *pdxlnProjList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopPrL);
	
	// create a copy of the required output columns
	CColRefSet *pcrsOutput = GPOS_NEW(m_pmp) CColRefSet(m_pmp, *pcrsRequired);
	
	if (NULL != pexprProjList)
	{
		// translate defined columns from project list
		for (ULONG ul = 0; ul < pexprProjList->UlArity(); ul++)
		{
			CExpression *pexprProjElem = (*pexprProjList)[ul];
			
			// translate proj elem
			CDXLNode *pdxlnProjElem = PdxlnProjElem(pexprProjElem);
			pdxlnProjList->AddChild(pdxlnProjElem);

			// exclude proj elem col ref from the output column set as it has been
			// processed already
			const CScalarProjectElement *popScPrEl =
					CScalarProjectElement::PopConvert(pexprProjElem->Pop());
			pcrsOutput->Exclude(popScPrEl->Pcr());
		}
	}
	
	// translate columns which remained after processing the project list: those
	// are columns passed from the level below
	CColRefSetIter crsi(*pcrsOutput);
	while(crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		CDXLNode *pdxlnPrEl = CTranslatorExprToDXLUtils::PdxlnProjElem(m_pmp, m_phmcrdxln, pcr);
		pdxlnProjList->AddChild(pdxlnPrEl);
	}
	
	// cleanup
	pcrsOutput->Release();
	
	return pdxlnProjList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnProjListFromConstTableGet
//
//	@doc:
//		Construct a project list node by creating references to the columns
//		of the given project list of the child node 
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnProjListFromConstTableGet
	(
	DrgPcr *pdrgpcrReqOutput, 
	DrgPcr *pdrgpcrCTGOutput,
	DrgPdatum *pdrgpdatumValues
	)
{
	GPOS_ASSERT(NULL != pdrgpcrCTGOutput);
	GPOS_ASSERT(NULL != pdrgpdatumValues);
	GPOS_ASSERT(pdrgpcrCTGOutput->UlLength() == pdrgpdatumValues->UlLength());
	
	CDXLNode *pdxlnProjList = NULL;
	CColRefSet *pcrsOutput = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrsOutput->Include(pdrgpcrCTGOutput);

	if (NULL != pdrgpcrReqOutput)
	{
		const ULONG ulArity = pdrgpcrReqOutput->UlLength();
		DrgPdatum *pdrgpdatumOrdered = GPOS_NEW(m_pmp) DrgPdatum(m_pmp);

		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			CColRef *pcr = (*pdrgpcrReqOutput)[ul];
			ULONG ulPos = UlPosInArray(pcr, pdrgpcrCTGOutput);
			GPOS_ASSERT(ulPos < pdrgpcrCTGOutput->UlLength());
			IDatum *pdatum = (*pdrgpdatumValues)[ulPos];
			pdatum->AddRef();
			pdrgpdatumOrdered->Append(pdatum);
			pcrsOutput->Exclude(pcr);
		}

		pdxlnProjList = PdxlnProjListFromConstTableGet(NULL, pdrgpcrReqOutput, pdrgpdatumOrdered);
		pdrgpdatumOrdered->Release();
	}
	else
	{
		pdxlnProjList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarProjList(m_pmp));
	}

	// construct project elements for columns which remained after processing the required list
	CColRefSetIter crsi(*pcrsOutput);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		ULONG ulPos = UlPosInArray(pcr, pdrgpcrCTGOutput);
		GPOS_ASSERT(ulPos < pdrgpcrCTGOutput->UlLength());
		IDatum *pdatum = (*pdrgpdatumValues)[ulPos];
		CDXLScalarConstValue *pdxlopConstValue = pcr->Pmdtype()->PdxlopScConst(m_pmp, pdatum);
		CDXLNode *pdxlnPrEl = PdxlnProjElem(pcr, GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopConstValue));
		pdxlnProjList->AddChild(pdxlnPrEl);
	}

	// cleanup
	pcrsOutput->Release();

	return pdxlnProjList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnProjElem
//
//	@doc:
//		 Create a project elem from a given col ref and a value
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnProjElem
	(
	const CColRef *pcr,
	CDXLNode *pdxlnValue
	)
{
	GPOS_ASSERT(NULL != pcr);
	
	CMDName *pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pcr->Name().Pstr());
	CDXLNode *pdxlnPrEl = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarProjElem(m_pmp, pcr->UlId(), pmdname));
	
	// attach scalar id expression to proj elem
	pdxlnPrEl->AddChild(pdxlnValue);
	
	return pdxlnPrEl;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnProjElem
//
//	@doc:
//		 Create a project elem from a given col ref
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnProjElem
	(
	const CExpression *pexprProjElem
	)
{
	GPOS_ASSERT(NULL != pexprProjElem && 1 == pexprProjElem->UlArity());
	
	CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexprProjElem->Pop());
	
	CColRef *pcr = popScPrEl->Pcr();
	
	CMDName *pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pcr->Name().Pstr());
	CDXLNode *pdxlnPrEl = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarProjElem(m_pmp, pcr->UlId(), pmdname));
	
	CExpression *pexprChild = (*pexprProjElem)[0];
	CDXLNode *pdxlnChild = PdxlnScalar(pexprChild);
	
	pdxlnPrEl->AddChild(pdxlnChild);
	
	return pdxlnPrEl;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnSortColList
//
//	@doc:
//		 Create a dxl sort column list node from a given order spec
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnSortColList
	(
	const COrderSpec *pos
	)
{
	GPOS_ASSERT(NULL != pos);
	
	CDXLNode *pdxlnSortColList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarSortColList(m_pmp));
	
	for (ULONG ul = 0; ul < pos->UlSortColumns(); ul++)
	{
		// get sort column components
		IMDId *pmdidSortOp = pos->PmdidSortOp(ul);
		pmdidSortOp->AddRef();
		
		const CColRef *pcr = pos->Pcr(ul);
		
		COrderSpec::ENullTreatment ent = pos->Ent(ul);
		GPOS_ASSERT(COrderSpec::EntFirst == ent || COrderSpec::EntLast == ent || COrderSpec::EntAuto == ent);
		
		// get sort operator name
		const IMDScalarOp *pmdscop = m_pmda->Pmdscop(pmdidSortOp);
		
		CWStringConst *pstrSortOpName = 
				GPOS_NEW(m_pmp) CWStringConst(m_pmp, pmdscop->Mdname().Pstr()->Wsz());
		
		BOOL fSortNullsFirst = false;
		if (COrderSpec::EntFirst == ent)
		{
			fSortNullsFirst = true;
		}

		CDXLScalarSortCol *pdxlopSortCol =
				GPOS_NEW(m_pmp) CDXLScalarSortCol
							(
							m_pmp, 
							pcr->UlId(), 
							pmdidSortOp,
							pstrSortOpName,
							fSortNullsFirst
							);
		
		CDXLNode *pdxlnSortCol = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopSortCol);
		pdxlnSortColList->AddChild(pdxlnSortCol);
	}
	
	return pdxlnSortColList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnHashExprList
//
//	@doc:
//		 Create a dxl hash expr list node from a given array of column references
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnHashExprList
	(
	const DrgPexpr *pdrgpexpr
	)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	
	CDXLNode *pdxlnHashExprList = 
			GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarHashExprList(m_pmp));
	
	for (ULONG ul = 0; ul < pdrgpexpr->UlLength(); ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];
		CScalar *popScalar = CScalar::PopConvert(pexpr->Pop());
		IMDId *pmdidType = popScalar->PmdidType();
		pmdidType->AddRef();
	
		// constrct a hash expr node for the col ref
		CDXLNode *pdxlnHashExpr = GPOS_NEW(m_pmp) CDXLNode
												(
												m_pmp,
												GPOS_NEW(m_pmp) CDXLScalarHashExpr
															(
															m_pmp,
															pmdidType
															)
												);
												
		pdxlnHashExpr->AddChild(PdxlnScalar(pexpr));
		
		pdxlnHashExprList->AddChild(pdxlnHashExpr);
	}
	
	return pdxlnHashExprList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdxlnSortColList
//
//	@doc:
//		 Create a dxl sort column list node for a given motion operator
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorExprToDXL::PdxlnSortColList
	(
	CExpression *pexprMotion
	)
{
	CDXLNode *pdxlnSortColList = NULL;
	if (COperator::EopPhysicalMotionGather == pexprMotion->Pop()->Eopid())
	{
		// construct a sorting column list node
		CPhysicalMotionGather *popGather = CPhysicalMotionGather::PopConvert(pexprMotion->Pop());
		pdxlnSortColList = PdxlnSortColList(popGather->Pos());
	}
	else
	{
		pdxlnSortColList = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarSortColList(m_pmp));
	}
	
	return pdxlnSortColList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdrgpiOutputSegIds
//
//	@doc:
//		Construct an array with output segment indices for the given Motion
//		expression.
//
//---------------------------------------------------------------------------
DrgPi *
CTranslatorExprToDXL::PdrgpiOutputSegIds
	(
	CExpression *pexprMotion
	)
{
	DrgPi *pdrgpi = NULL;
	
	COperator *pop = pexprMotion->Pop();
	
	switch (pop->Eopid())
	{
		case COperator::EopPhysicalMotionGather:
		{
			CPhysicalMotionGather *popGather = CPhysicalMotionGather::PopConvert(pop);
		
			pdrgpi = GPOS_NEW(m_pmp) DrgPi(m_pmp);
			INT iSegmentId = m_iMasterId;
			
			if (CDistributionSpecSingleton::EstSegment == popGather->Est())
			{
				// gather to first segment
				iSegmentId = *((*m_pdrgpiSegments)[0]);
			}
			pdrgpi->Append(GPOS_NEW(m_pmp) INT(iSegmentId));
			break;
		}
		case COperator::EopPhysicalMotionBroadcast:
		case COperator::EopPhysicalMotionHashDistribute:
		case COperator::EopPhysicalMotionRoutedDistribute:
		case COperator::EopPhysicalMotionRandom:
		{
			m_pdrgpiSegments->AddRef();
			pdrgpi = m_pdrgpiSegments;
			break;
		}
		default:
			GPOS_ASSERT(!"Unrecognized motion operator");
	}
	
	GPOS_ASSERT(NULL != pdrgpi);
	
	return pdrgpi;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::PdrgpiInputSegIds
//
//	@doc:
//		Construct an array with input segment indices for the given Motion
//		expression.
//
//---------------------------------------------------------------------------
DrgPi *
CTranslatorExprToDXL::PdrgpiInputSegIds
	(
	CExpression *pexprMotion
	)
{
	GPOS_ASSERT(1 == pexprMotion->UlArity());

	// derive the distribution of child expression
	CExpression *pexprChild = (*pexprMotion)[0];
	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan(pexprChild->PdpDerive());
	CDistributionSpec *pds = pdpplan->Pds();

	if (CDistributionSpec::EdtSingleton == pds->Edt() || CDistributionSpec::EdtStrictSingleton == pds->Edt())
	{
		DrgPi *pdrgpi = GPOS_NEW(m_pmp) DrgPi(m_pmp);
		INT iSegmentId = m_iMasterId;
		CDistributionSpecSingleton *pdss = CDistributionSpecSingleton::PdssConvert(pds);
		if (!pdss->FOnMaster())
		{
			// non-master singleton is currently fixed to the first segment
			iSegmentId = *((*m_pdrgpiSegments)[0]);
		}
		pdrgpi->Append(GPOS_NEW(m_pmp) INT(iSegmentId));
		return pdrgpi;
	}

	if (CUtils::FDuplicateHazardMotion(pexprMotion))
	{
		// if Motion is duplicate-hazard, we have to read from one input segment
		// to avoid generating duplicate values
		DrgPi *pdrgpi = GPOS_NEW(m_pmp) DrgPi(m_pmp);
		INT iSegmentId = *((*m_pdrgpiSegments)[0]);
		pdrgpi->Append(GPOS_NEW(m_pmp) INT(iSegmentId));
		return pdrgpi;
	}

	m_pdrgpiSegments->AddRef();
	return m_pdrgpiSegments;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorExprToDXL::UlPosInArray
//
//	@doc:
//		Find position of colref in the array
//
//---------------------------------------------------------------------------
ULONG
CTranslatorExprToDXL::UlPosInArray
	(
	const CColRef *pcr,
	const DrgPcr *pdrgpcr
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpcr);
	GPOS_ASSERT(NULL != pcr);
	
	const ULONG ulSize = pdrgpcr->UlLength();
	
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		if (pcr == (*pdrgpcr)[ul])
		{
			return ul;
		}
	}
	
	// not found
	return ulSize;
}

// EOF
