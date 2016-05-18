//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorDXLToExpr.cpp
//
//	@doc:
//		Implementation of the methods used to translate a DXL tree into Expr tree.
//		All translator methods allocate memory in the provided memory pool, and
//		the caller is responsible for freeing it.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/common/CAutoTimer.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDCast.h"
#include "naucrates/md/CMDRelationCtasGPDB.h"
#include "naucrates/md/CMDProviderMemory.h"

#include "naucrates/dxl/operators/dxlops.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CEnfdOrder.h"
#include "gpopt/base/CEnfdDistribution.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/metadata/CColumnDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXLUtils.h"
#include "gpopt/exception.h"

#include "naucrates/traceflags/traceflags.h"

#define GPDB_DENSE_RANK_OID 7002
#define GPDB_PERCENT_RANK_OID 7003
#define GPDB_CUME_DIST_OID 7004
#define GPDB_NTILE_INT4_OID 7005
#define GPDB_NTILE_INT8_OID 7006
#define GPDB_NTILE_NUMERIC_OID 7007
#define GPOPT_ACTION_INSERT 0
#define GPOPT_ACTION_DELETE 1

using namespace gpos;
using namespace gpnaucrates;
using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::CTranslatorDXLToExpr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CTranslatorDXLToExpr::CTranslatorDXLToExpr
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	BOOL fInitColumnFactory
	)
	:
	m_pmp(pmp),
	m_sysid(IMDId::EmdidGPDB, GPMD_GPDB_SYSID),
	m_pmda(pmda),
	m_phmulcr(NULL),
	m_phmululCTE(NULL),
	m_pdrgpulOutputColRefs(NULL),
	m_pdrgpmdname(NULL),
	m_phmulpdxlnCTEProducer(NULL),
	m_ulCTEId(ULONG_MAX),
	m_pcf(NULL)
{
	// initialize hash tables
	m_phmulcr = GPOS_NEW(m_pmp) HMUlCr(m_pmp);

	// initialize hash tables
	m_phmululCTE = GPOS_NEW(m_pmp) HMUlUl(m_pmp);

	const ULONG ulSize = GPOS_ARRAY_SIZE(m_rgpfTranslators);
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		m_rgpfTranslators[ul] = NULL;
	}

	InitTranslators();

	if (fInitColumnFactory)
	{
		// get column factory from optimizer context object
		m_pcf = COptCtxt::PoctxtFromTLS()->Pcf();

		GPOS_ASSERT(NULL != m_pcf);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::~CTranslatorDXLToExpr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CTranslatorDXLToExpr::~CTranslatorDXLToExpr()
{
	m_phmulcr->Release();
	m_phmululCTE->Release();
	CRefCount::SafeRelease(m_pdrgpulOutputColRefs);
	CRefCount::SafeRelease(m_pdrgpmdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::InitTranslators
//
//	@doc:
//		Initialize index of scalar translators
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToExpr::InitTranslators()
{
	// array mapping operator type to translator function
	STranslatorMapping rgTranslators[] = 
	{
		{EdxlopLogicalGet,			&gpopt::CTranslatorDXLToExpr::PexprLogicalGet},
		{EdxlopLogicalExternalGet,	&gpopt::CTranslatorDXLToExpr::PexprLogicalGet},
		{EdxlopLogicalTVF,			&gpopt::CTranslatorDXLToExpr::PexprLogicalTVF},
		{EdxlopLogicalSelect, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalSelect},
		{EdxlopLogicalProject, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalProject},
		{EdxlopLogicalCTEAnchor,	&gpopt::CTranslatorDXLToExpr::PexprLogicalCTEAnchor},
		{EdxlopLogicalCTEProducer,	&gpopt::CTranslatorDXLToExpr::PexprLogicalCTEProducer},
		{EdxlopLogicalCTEConsumer,	&gpopt::CTranslatorDXLToExpr::PexprLogicalCTEConsumer},
		{EdxlopLogicalGrpBy, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalGroupBy},
		{EdxlopLogicalLimit, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalLimit},
		{EdxlopLogicalJoin, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalJoin},
		{EdxlopLogicalConstTable, 	&gpopt::CTranslatorDXLToExpr::PexprLogicalConstTableGet},
		{EdxlopLogicalSetOp, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalSetOp},
		{EdxlopLogicalWindow, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalSeqPr},
		{EdxlopLogicalInsert, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalInsert},
		{EdxlopLogicalDelete, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalDelete},
		{EdxlopLogicalUpdate, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalUpdate},
		{EdxlopLogicalCTAS, 		&gpopt::CTranslatorDXLToExpr::PexprLogicalCTAS},
		{EdxlopScalarIdent,			&gpopt::CTranslatorDXLToExpr::PexprScalarIdent},
		{EdxlopScalarCmp, 			&gpopt::CTranslatorDXLToExpr::PexprScalarCmp},
		{EdxlopScalarOpExpr, 		&gpopt::CTranslatorDXLToExpr::PexprScalarOp},
		{EdxlopScalarDistinct, 		&gpopt::CTranslatorDXLToExpr::PexprScalarIsDistinctFrom},
		{EdxlopScalarConstValue, 	&gpopt::CTranslatorDXLToExpr::PexprScalarConst},
		{EdxlopScalarBoolExpr, 		&gpopt::CTranslatorDXLToExpr::PexprScalarBoolOp},
		{EdxlopScalarFuncExpr, 		&gpopt::CTranslatorDXLToExpr::PexprScalarFunc},
		{EdxlopScalarMinMax, 		&gpopt::CTranslatorDXLToExpr::PexprScalarMinMax},
		{EdxlopScalarAggref, 		&gpopt::CTranslatorDXLToExpr::PexprAggFunc},
		{EdxlopScalarWindowRef, 	&gpopt::CTranslatorDXLToExpr::PexprWindowFunc},
		{EdxlopScalarNullTest, 		&gpopt::CTranslatorDXLToExpr::PexprScalarNullTest},
		{EdxlopScalarNullIf, 		&gpopt::CTranslatorDXLToExpr::PexprScalarNullIf},
		{EdxlopScalarBooleanTest, 	&gpopt::CTranslatorDXLToExpr::PexprScalarBooleanTest},
		{EdxlopScalarIfStmt, 		&gpopt::CTranslatorDXLToExpr::PexprScalarIf},
		{EdxlopScalarSwitch, 		&gpopt::CTranslatorDXLToExpr::PexprScalarSwitch},
		{EdxlopScalarSwitchCase,	&gpopt::CTranslatorDXLToExpr::PexprScalarSwitchCase},
		{EdxlopScalarCaseTest,		&gpopt::CTranslatorDXLToExpr::PexprScalarCaseTest},
		{EdxlopScalarCoalesce, 		&gpopt::CTranslatorDXLToExpr::PexprScalarCoalesce},
		{EdxlopScalarCast, 			&gpopt::CTranslatorDXLToExpr::PexprScalarCast},
		{EdxlopScalarCoerceToDomain,&gpopt::CTranslatorDXLToExpr::PexprScalarCoerceToDomain},
		{EdxlopScalarSubquery, 		&gpopt::CTranslatorDXLToExpr::PexprScalarSubquery},
		{EdxlopScalarSubqueryAny, 	&gpopt::CTranslatorDXLToExpr::PexprScalarSubqueryQuantified},
		{EdxlopScalarSubqueryAll, 	&gpopt::CTranslatorDXLToExpr::PexprScalarSubqueryQuantified},
		{EdxlopScalarArray, 		&gpopt::CTranslatorDXLToExpr::PexprArray},
		{EdxlopScalarArrayComp, 	&gpopt::CTranslatorDXLToExpr::PexprArrayCmp},
		{EdxlopScalarArrayRef, 		&gpopt::CTranslatorDXLToExpr::PexprArrayRef},
		{EdxlopScalarArrayRefIndexList, &gpopt::CTranslatorDXLToExpr::PexprArrayRefIndexList},
	};

	const ULONG ulTranslators = GPOS_ARRAY_SIZE(rgTranslators);
	
	for (ULONG ul = 0; ul < ulTranslators; ul++)
	{
		STranslatorMapping elem = rgTranslators[ul];
		m_rgpfTranslators[elem.edxlopid] = elem.pf;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PdrgpulOutputColRefs
//
//	@doc:
// 		Return the array of query output column reference id
//
//---------------------------------------------------------------------------
DrgPul *
CTranslatorDXLToExpr::PdrgpulOutputColRefs()
{
	GPOS_ASSERT(NULL != m_pdrgpulOutputColRefs);
	return m_pdrgpulOutputColRefs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Pexpr
//
//	@doc:
//		Translate a DXL tree into an Expr Tree
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::Pexpr
	(
	const CDXLNode *pdxln,
	const DrgPdxln *pdrgpdxlnQueryOutput,
	const DrgPdxln *pdrgpdxlnCTE
	)
{
	GPOS_ASSERT(NULL == m_pdrgpulOutputColRefs);
	GPOS_ASSERT(NULL == m_phmulpdxlnCTEProducer);
	GPOS_ASSERT(NULL != pdxln && NULL != pdxln->Pdxlop());
	GPOS_ASSERT(NULL != pdrgpdxlnQueryOutput);
	
	m_phmulpdxlnCTEProducer = GPOS_NEW(m_pmp) HMUlPdxln(m_pmp);
	const ULONG ulCTEs = pdrgpdxlnCTE->UlLength();
	for (ULONG ul = 0; ul < ulCTEs; ul++)
	{
		CDXLNode *pdxlnCTE = (*pdrgpdxlnCTE)[ul];
		CDXLLogicalCTEProducer *pdxlopCTEProducer = CDXLLogicalCTEProducer::PdxlopConvert(pdxlnCTE->Pdxlop());

		pdxlnCTE->AddRef();
#ifdef GPOS_DEBUG
		BOOL fres =
#endif // GPOS_DEBUG
				m_phmulpdxlnCTEProducer->FInsert(GPOS_NEW(m_pmp) ULONG(pdxlopCTEProducer->UlId()), pdxlnCTE);
		GPOS_ASSERT(fres);
	}

	// translate main DXL tree
	CExpression *pexpr = Pexpr(pdxln);
	GPOS_ASSERT(NULL != pexpr);

	m_phmulpdxlnCTEProducer->Release();
	m_phmulpdxlnCTEProducer = NULL;

	// generate the array of output column reference ids and column names
	m_pdrgpulOutputColRefs = GPOS_NEW(m_pmp) DrgPul(m_pmp);
	m_pdrgpmdname = GPOS_NEW(m_pmp) DrgPmdname(m_pmp);

	BOOL fGenerateRequiredColumns = COperator::EopLogicalUpdate != pexpr->Pop()->Eopid();
	
	const ULONG ulLen = pdrgpdxlnQueryOutput->UlSafeLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CDXLNode *pdxlnIdent = (*pdrgpdxlnQueryOutput)[ul];

		// get dxl scalar identifier
		CDXLScalarIdent *pdxlopIdent = CDXLScalarIdent::PdxlopConvert(pdxlnIdent->Pdxlop());

		// get the dxl column reference
		const CDXLColRef *pdxlcr = pdxlopIdent->Pdxlcr();
		GPOS_ASSERT(NULL != pdxlcr);
		const ULONG ulColId = pdxlcr->UlID();

		// get its column reference from the hash map
		const CColRef *pcr = PcrLookup(m_phmulcr, ulColId);
		
		if (fGenerateRequiredColumns)
		{
			const ULONG ulColRefId =  pcr->UlId();
			ULONG *pulCopy = GPOS_NEW(m_pmp) ULONG(ulColRefId);
			// add to the array of output column reference ids
			m_pdrgpulOutputColRefs->Append(pulCopy);
	
			// get the column names and add it to the array of output column names
			CMDName *pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pdxlcr->Pmdname()->Pstr());
			m_pdrgpmdname->Append(pmdname);
		}
	}

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Pexpr
//
//	@doc:
//		Translates a DXL tree into a Expr Tree
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::Pexpr
	(
	const CDXLNode *pdxln
	)
{
	// recursive function - check stack
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pdxln && NULL != pdxln->Pdxlop());

	CDXLOperator *pdxlop = pdxln->Pdxlop();

	CExpression *pexpr = NULL;
	switch (pdxlop->Edxloperatortype())
	{
		case EdxloptypeLogical:
				pexpr = PexprLogical(pdxln);
				break;

		case EdxloptypeScalar:
				pexpr = PexprScalar(pdxln);
				break;

		default:
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, pdxln->Pdxlop()->PstrOpName()->Wsz());
	}

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprTranslateQuery
//
//	@doc:
// 		 Main driver for translating dxl query with its associated output
//		columns and CTEs
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprTranslateQuery
	(
	const CDXLNode *pdxln,
	const DrgPdxln *pdrgpdxlnQueryOutput,
	const DrgPdxln *pdrgpdxlnCTE
	)
{
	CAutoTimer at("\n[OPT]: DXL To Expr Translation Time", GPOS_FTRACE(EopttracePrintOptStats));

	return Pexpr(pdxln, pdrgpdxlnQueryOutput, pdrgpdxlnCTE);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprTranslateScalar
//
//	@doc:
// 		 Translate a dxl scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprTranslateScalar
	(
	const CDXLNode *pdxln,
	DrgPcr *pdrgpcr,
	DrgPul *pdrgpul
	)
{
	GPOS_ASSERT_IMP(NULL != pdrgpul, NULL != pdrgpcr);
	GPOS_ASSERT_IMP(NULL != pdrgpul, pdrgpul->UlLength() == pdrgpcr->UlLength());

	CAutoTimer at("\n[OPT]: Scalar DXL To Scalar Expr Translation Time", GPOS_FTRACE(EopttracePrintOptStats));

	if (EdxloptypeScalar != pdxln->Pdxlop()->Edxloperatortype())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnexpectedOp, pdxln->Pdxlop()->PstrOpName()->Wsz());
	}
	
	if (NULL != pdrgpcr)
	{
		const ULONG ulLen = pdrgpcr->UlLength();
		for (ULONG ul = 0; ul < ulLen; ul++)
		{
			CColRef *pcr = (*pdrgpcr)[ul];
			// copy key
			ULONG *pulKey = NULL;
			if (NULL == pdrgpul)
			{
				pulKey = GPOS_NEW(m_pmp) ULONG(ul+1);
			}
			else
			{
				pulKey = GPOS_NEW(m_pmp) ULONG(*((*pdrgpul)[ul]) + 1);
			}
	#ifdef GPOS_DEBUG
			BOOL fres =
	#endif // GPOS_DEBUG
					m_phmulcr->FInsert(pulKey, pcr);
			GPOS_ASSERT(fres);
		}
	}

	return PexprScalar(pdxln);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogical
//
//	@doc:
//		Translates a DXL Logical Op into a Expr
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogical
	(
	const CDXLNode *pdxln
	)
{
	// recursive function - check stack
	GPOS_CHECK_STACK_SIZE;

	GPOS_ASSERT(NULL != pdxln);
	GPOS_ASSERT(EdxloptypeLogical == pdxln->Pdxlop()->Edxloperatortype());
	CDXLOperator *pdxlop = pdxln->Pdxlop();

	ULONG ulOpId =  (ULONG) pdxlop->Edxlop();
	PfPexpr pf = m_rgpfTranslators[ulOpId];

	if (NULL == pf)
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, pdxlop->PstrOpName()->Wsz());
	}
	
	return (this->* pf)(pdxln);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalTVF
//
//	@doc:
// 		Create a logical TVF expression from its DXL representation
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalTVF
	(
	const CDXLNode *pdxln
	)
{
	CDXLLogicalTVF *pdxlop = CDXLLogicalTVF::PdxlopConvert(pdxln->Pdxlop());
	GPOS_ASSERT(NULL != pdxlop->Pmdname()->Pstr());

	// populate column information
	const ULONG ulColumns = pdxlop->UlArity();
	GPOS_ASSERT(0 < ulColumns);

	DrgPcoldesc *pdrgpcoldesc = GPOS_NEW(m_pmp) DrgPcoldesc(m_pmp);

	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		const CDXLColDescr *pdxlcoldesc = pdxlop->Pdxlcd(ul);
		GPOS_ASSERT(pdxlcoldesc->PmdidType()->FValid());

		const IMDType *pmdtype = m_pmda->Pmdtype(pdxlcoldesc->PmdidType());

		GPOS_ASSERT(NULL != pdxlcoldesc->Pmdname()->Pstr()->Wsz());
		CWStringConst strColName(m_pmp, pdxlcoldesc->Pmdname()->Pstr()->Wsz());

		INT iAttNo = pdxlcoldesc->IAttno();
		CColumnDescriptor *pcoldesc = GPOS_NEW(m_pmp) CColumnDescriptor
													(
													m_pmp,
													pmdtype,
													CName(m_pmp, &strColName),
													iAttNo,
													true, // fNullable
													pdxlcoldesc->UlWidth()
													);
		pdrgpcoldesc->Append(pcoldesc);
	}

	// create a logical TVF operator
	IMDId *pmdidFunc = pdxlop->PmdidFunc();
	pmdidFunc->AddRef();

	IMDId *pmdidRetType = pdxlop->PmdidRetType();
	pmdidRetType->AddRef();
	CLogicalTVF *popTVF = GPOS_NEW(m_pmp) CLogicalTVF
										(
										m_pmp,
										pmdidFunc,
										pmdidRetType,
										GPOS_NEW(m_pmp) CWStringConst(m_pmp, pdxlop->Pmdname()->Pstr()->Wsz()),
										pdrgpcoldesc
										);

	// create expression containing the logical TVF operator
	CExpression *pexpr = NULL;
	const ULONG ulArity = pdxln->UlArity();
	if (0 < ulArity)
	{
		// translate function arguments
		DrgPexpr *pdrgpexprArgs = PdrgpexprChildren(pdxln);

		pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, popTVF, pdrgpexprArgs);
	}
	else
	{
		// function has no arguments
		pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, popTVF);
	}

	// construct the mapping between the DXL ColId and CColRef
	ConstructDXLColId2ColRefMapping(pdxlop->Pdrgpdxlcd(), popTVF->PdrgpcrOutput());

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalGet
//
//	@doc:
// 		Create a Expr logical get from a DXL logical get
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalGet
	(
	const CDXLNode *pdxln
	)
{
	CDXLOperator *pdxlop = pdxln->Pdxlop();
	Edxlopid edxlopid = pdxlop->Edxlop();

	// translate the table descriptor
	CDXLTableDescr *pdxltabdesc = CDXLLogicalGet::PdxlopConvert(pdxlop)->Pdxltabdesc();

	GPOS_ASSERT(NULL != pdxltabdesc);
	GPOS_ASSERT(NULL != pdxltabdesc->Pmdname()->Pstr());

	CTableDescriptor *ptabdesc = Ptabdesc(pdxltabdesc);

	CWStringConst strAlias(m_pmp, pdxltabdesc->Pmdname()->Pstr()->Wsz());

	// create a logical get or dynamic get operator
	CName *pname = GPOS_NEW(m_pmp) CName(m_pmp, CName(&strAlias));
	CLogical *popGet = NULL;
	DrgPcr *pdrgpcr = NULL; 

	const IMDRelation *pmdrel = m_pmda->Pmdrel(pdxltabdesc->Pmdid());
	if (pmdrel->FPartitioned())
	{
		GPOS_ASSERT(EdxlopLogicalGet == edxlopid);

		// generate a part index id
		ULONG ulPartIndexId = COptCtxt::PoctxtFromTLS()->UlPartIndexNextVal();
		popGet = GPOS_NEW(m_pmp) CLogicalDynamicGet(m_pmp, pname, ptabdesc, ulPartIndexId);	
		CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert(popGet);

		// get the output column references from the dynamic get
		pdrgpcr = popDynamicGet->PdrgpcrOutput();

		// if there are no indices, we only generate a dummy partition constraint because
		// the constraint might be expensive to compute and it is not needed
		BOOL fDummyConstraint = 0 == pmdrel->UlIndices();
		CPartConstraint *ppartcnstr = CUtils::PpartcnstrFromMDPartCnstr
												(
												m_pmp,
												m_pmda,
												popDynamicGet->PdrgpdrgpcrPart(),
												pmdrel->Pmdpartcnstr(),
												pdrgpcr,
												fDummyConstraint
												);
		popDynamicGet->SetPartConstraint(ppartcnstr);
	}
	else
	{
		if (EdxlopLogicalGet == edxlopid)
		{
			popGet = GPOS_NEW(m_pmp) CLogicalGet(m_pmp, pname, ptabdesc);
		}
		else
		{
			GPOS_ASSERT(EdxlopLogicalExternalGet == edxlopid);
			popGet = GPOS_NEW(m_pmp) CLogicalExternalGet(m_pmp, pname, ptabdesc);
		}

		// get the output column references
		pdrgpcr = CLogicalGet::PopConvert(popGet)->PdrgpcrOutput();
	}

	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, popGet);

	GPOS_ASSERT(NULL != pdrgpcr);
	GPOS_ASSERT(pdrgpcr->UlSafeLength() == pdxltabdesc->UlArity());

	const ULONG ulColumns = pdrgpcr->UlLength();
	// construct the mapping between the DXL ColId and CColRef
	for(ULONG ul = 0; ul < ulColumns ; ul++)
	{
		CColRef *pcr = (*pdrgpcr)[ul];
		const CDXLColDescr *pdxlcd = pdxltabdesc->Pdxlcd(ul);
		GPOS_ASSERT(NULL != pcr);
		GPOS_ASSERT(NULL != pdxlcd && !pdxlcd-> FDropped());

		// copy key
		ULONG *pulKey = GPOS_NEW(m_pmp) ULONG(pdxlcd->UlID());
		BOOL fres = m_phmulcr->FInsert(pulKey, pcr);

		if (!fres)
		{
			GPOS_DELETE(pulKey);
		}
	}

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalSetOp
//
//	@doc:
// 		Create a logical set operator from a DXL set operator
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalSetOp
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);

	CDXLLogicalSetOp *pdxlop = CDXLLogicalSetOp::PdxlopConvert(pdxln->Pdxlop());

#ifdef GPOS_DEBUG
	const ULONG ulArity = pdxln->UlArity();
#endif // GPOS_DEBUG

	GPOS_ASSERT(2 <= ulArity);
	GPOS_ASSERT(ulArity == pdxlop->UlChildren());

	// array of input column reference
	DrgDrgPcr *pdrgdrgpcrInput = GPOS_NEW(m_pmp) DrgDrgPcr(m_pmp);
		// array of output column descriptors
	DrgPul *pdrgpulOutput = GPOS_NEW(m_pmp) DrgPul(m_pmp);
	
	DrgPexpr *pdrgpexpr = PdrgpexprPreprocessSetOpInputs(pdxln, pdrgdrgpcrInput, pdrgpulOutput);

	// create an array of output column references
	DrgPcr *pdrgpcrOutput = CTranslatorDXLToExprUtils::Pdrgpcr(m_pmp, m_phmulcr, pdrgpulOutput /*array of colids of the first child*/);

	pdrgpulOutput->Release();

	CLogicalSetOp *pop = NULL;
	switch (pdxlop->Edxlsetoptype())
	{
		case EdxlsetopUnion:
				{
					pop = GPOS_NEW(m_pmp) CLogicalUnion(m_pmp, pdrgpcrOutput, pdrgdrgpcrInput);
					break;
				}

		case EdxlsetopUnionAll:
				{
					pop = GPOS_NEW(m_pmp) CLogicalUnionAll(m_pmp, pdrgpcrOutput, pdrgdrgpcrInput);
					break;
				}

		case EdxlsetopDifference:
				{
					pop = GPOS_NEW(m_pmp) CLogicalDifference(m_pmp, pdrgpcrOutput, pdrgdrgpcrInput);
					break;
				}

		case EdxlsetopIntersect:
				{
					pop = GPOS_NEW(m_pmp) CLogicalIntersect(m_pmp, pdrgpcrOutput, pdrgdrgpcrInput);
					break;
				}

		case EdxlsetopDifferenceAll:
				{
					pop = GPOS_NEW(m_pmp) CLogicalDifferenceAll(m_pmp, pdrgpcrOutput, pdrgdrgpcrInput);
					break;
				}

		case EdxlsetopIntersectAll:
				{
					pop = GPOS_NEW(m_pmp) CLogicalIntersectAll(m_pmp, pdrgpcrOutput, pdrgdrgpcrInput);
					break;
				}

		default:
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, pdxlop->PstrOpName()->Wsz());
	}

	GPOS_ASSERT(NULL != pop);

	return GPOS_NEW(m_pmp) CExpression(m_pmp, pop, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprCastPrjElem
//
//	@doc:
//		Return a project element on a cast expression
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprCastPrjElem
	(
	IMDId *pmdidSource,
	IMDId *pmdidDest,
	const CColRef *pcrToCast,
	CColRef *pcrToReturn
	)
{
	const IMDCast *pmdcast = m_pmda->Pmdcast(pmdidSource, pmdidDest);
	pmdidDest->AddRef();
	pmdcast->PmdidCastFunc()->AddRef();

	CExpression *pexprCast =
		GPOS_NEW(m_pmp) CExpression
			(
			m_pmp,
			GPOS_NEW(m_pmp) CScalarCast(m_pmp, pmdidDest, pmdcast->PmdidCastFunc(), pmdcast->FBinaryCoercible()),
			GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarIdent(m_pmp, pcrToCast))
			);

	return
		GPOS_NEW(m_pmp) CExpression
				(
				m_pmp,
				GPOS_NEW(m_pmp) CScalarProjectElement(m_pmp, pcrToReturn),
				pexprCast
				);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::BuildSetOpChild
//
//	@doc:
//		Build expression and input columns of SetOp Child
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToExpr::BuildSetOpChild
	(
	const CDXLNode *pdxlnSetOp,
	ULONG ulChildIndex,
	CExpression **ppexprChild, // output: generated child expression
	DrgPcr **ppdrgpcrChild, // output: generated child input columns
	DrgPexpr **ppdrgpexprChildProjElems // output: project elements to remap child input columns
	)
{
	GPOS_ASSERT(NULL != pdxlnSetOp);
	GPOS_ASSERT(NULL != ppexprChild);
	GPOS_ASSERT(NULL != ppdrgpcrChild);
	GPOS_ASSERT(NULL != ppdrgpexprChildProjElems);
	GPOS_ASSERT(NULL == *ppdrgpexprChildProjElems);

	const CDXLLogicalSetOp *pdxlop = CDXLLogicalSetOp::PdxlopConvert(pdxlnSetOp->Pdxlop());
	const CDXLNode *pdxlnChild = (*pdxlnSetOp)[ulChildIndex];

	// array of project elements to remap child input columns
	*ppdrgpexprChildProjElems = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);

	// array of child input column
	*ppdrgpcrChild = GPOS_NEW(m_pmp) DrgPcr(m_pmp);

	// translate child
	*ppexprChild = PexprLogical(pdxlnChild);

	const DrgPul *pdrgpulInput = pdxlop->Pdrgpul(ulChildIndex);
	const ULONG ulInputCols = pdrgpulInput->UlLength();
	CColRefSet *pcrsChildOutput = CDrvdPropRelational::Pdprel((*ppexprChild)->PdpDerive())->PcrsOutput();
	for (ULONG ulColPos = 0; ulColPos < ulInputCols; ulColPos++)
	{
		// column identifier of the input column
		ULONG ulColId = *(*pdrgpulInput)[ulColPos];
		const CColRef *pcr = PcrLookup(m_phmulcr, ulColId);

		// corresponding output column descriptor
		const CDXLColDescr *pdxlcdOutput = pdxlop->Pdxlcd(ulColPos);

		// check if a cast function needs to be introduced
		IMDId *pmdidSource = pcr->Pmdtype()->Pmdid();
		IMDId *pmdidDest = pdxlcdOutput->PmdidType();
		if (FCastingUnknownType(pmdidSource, pmdidDest))
		{
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, GPOS_WSZ_LIT("Casting of columns of unknown data type"));
		}

		const IMDType *pmdtype = m_pmda->Pmdtype(pmdidDest);
		BOOL fEqualTypes = IMDId::FEqualMDId(pmdidSource, pmdidDest);
		BOOL fFirstChild = (0 == ulChildIndex);
		BOOL fUnionOrUnionAll = ((EdxlsetopUnionAll == pdxlop->Edxlsetoptype()) || (EdxlsetopUnion == pdxlop->Edxlsetoptype()));

		if (!pcrsChildOutput->FMember(pcr))
		{
			// input column is an outer reference, add a project element for input column

			// add the colref to the hash map between DXL ColId and colref as they can used above the setop
			CColRef *pcrNew = PcrCreate(pcr, pmdtype, fFirstChild, pdxlcdOutput->UlID());
			(*ppdrgpcrChild)->Append(pcrNew);

			CExpression *pexprChildProjElem = NULL;
			if (fEqualTypes)
			{
				// project child input column
				pexprChildProjElem =
						GPOS_NEW(m_pmp) CExpression
						(
						m_pmp,
						GPOS_NEW(m_pmp) CScalarProjectElement(m_pmp, pcrNew),
						GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarIdent(m_pmp, pcr))
						);
			}
			else
			{
				// introduce cast expression
				pexprChildProjElem = PexprCastPrjElem(pmdidSource, pmdidDest, pcr, pcrNew);
			}

			(*ppdrgpexprChildProjElems)->Append(pexprChildProjElem);
			continue;
		}

		if (fEqualTypes)
		{
			// no cast function needed, add the colref to the array of input colrefs
			(*ppdrgpcrChild)->Append(const_cast<CColRef*>(pcr));
			continue;
		}

		if (fUnionOrUnionAll || fFirstChild)
		{
			// add the colref to the hash map between DXL ColId and colref as they can used above the setop
			CColRef *pcrNew = PcrCreate(pcr, pmdtype, fFirstChild, pdxlcdOutput->UlID());
			(*ppdrgpcrChild)->Append(pcrNew);

			// introduce cast expression for input column
			CExpression *pexprChildProjElem = PexprCastPrjElem(pmdidSource, pmdidDest, pcr, pcrNew);
			(*ppdrgpexprChildProjElems)->Append(pexprChildProjElem);
		}
		else
		{
			(*ppdrgpcrChild)->Append(const_cast<CColRef*>(pcr));
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PdrgpexprPreprocessSetOpInputs
//
//	@doc:
//		Pre-process inputs to the set operator and add casting when needed
//
//---------------------------------------------------------------------------
DrgPexpr *
CTranslatorDXLToExpr::PdrgpexprPreprocessSetOpInputs
	(
	const CDXLNode *pdxln,
	DrgDrgPcr *pdrgdrgpcrInput,
	DrgPul *pdrgpulOutput
	)
{
	GPOS_ASSERT(NULL != pdxln);
	GPOS_ASSERT(NULL != pdrgdrgpcrInput);
	GPOS_ASSERT(NULL != pdrgpulOutput);
	
	// array of child expression
	DrgPexpr *pdrgpexpr = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);

	CDXLLogicalSetOp *pdxlop = CDXLLogicalSetOp::PdxlopConvert(pdxln->Pdxlop());

	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(2 <= ulArity);
	GPOS_ASSERT(ulArity == pdxlop->UlChildren());

	const ULONG ulOutputCols = pdxlop->UlArity();

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = NULL;
		DrgPcr *pdrgpcrInput = NULL;
		DrgPexpr *pdrgpexprChildProjElems = NULL;
		BuildSetOpChild(pdxln, ul, &pexprChild, &pdrgpcrInput, &pdrgpexprChildProjElems);
		GPOS_ASSERT(ulOutputCols == pdrgpcrInput->UlLength());
		GPOS_ASSERT(NULL != pexprChild);

		pdrgdrgpcrInput->Append(pdrgpcrInput);

		if (0 < pdrgpexprChildProjElems->UlLength())
		{
			CExpression *pexprChildProject = GPOS_NEW(m_pmp) CExpression
															(
															m_pmp,
															GPOS_NEW(m_pmp) CLogicalProject(m_pmp),
															pexprChild,
															GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarProjectList(m_pmp), pdrgpexprChildProjElems)
															);
			pdrgpexpr->Append(pexprChildProject);
		}
		else
		{
			pdrgpexpr->Append(pexprChild);
			pdrgpexprChildProjElems->Release();
		}
	}

	// create the set operation's array of output column identifiers
	for (ULONG ulOutputColPos = 0; ulOutputColPos < ulOutputCols; ulOutputColPos++)
	{
		const CDXLColDescr *pdxlcdOutput = pdxlop->Pdxlcd(ulOutputColPos);
		pdrgpulOutput->Append(GPOS_NEW(m_pmp) ULONG (pdxlcdOutput->UlID()));
	}
	
	return pdrgpexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::FCastingUnknownType
//
//	@doc:
//		Check if we currently support the casting of such column types
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToExpr::FCastingUnknownType
	(
	IMDId *pmdidSource,
	IMDId *pmdidDest
	)
{
	return ((pmdidSource->FEquals(&CMDIdGPDB::m_mdidUnknown) || pmdidDest->FEquals(&CMDIdGPDB::m_mdidUnknown)));
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PcrLookup
//
//	@doc:
// 		Look up the column reference in the hash map. We raise an exception if
//		the column is not found
//---------------------------------------------------------------------------
CColRef *
CTranslatorDXLToExpr::PcrLookup
	(
	HMUlCr *phmulcr,
	ULONG ulColId
	)
{
	GPOS_ASSERT(NULL != phmulcr);
	GPOS_ASSERT(ULONG_MAX != ulColId);

	// get its column reference from the hash map
	CColRef *pcr = phmulcr->PtLookup(&ulColId);
    if (NULL == pcr)
    {
    	GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2ExprAttributeNotFound, ulColId);
    }

	return pcr;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PcrCreate
//
//	@doc:
// 		Create new column reference and add to the hashmap maintaining
//		the mapping between DXL ColIds and column reference.
//
//---------------------------------------------------------------------------
CColRef *
CTranslatorDXLToExpr::PcrCreate
	(
	const CColRef *pcr,
	const IMDType *pmdtype,
	BOOL fStoreMapping,
	ULONG ulColId
	)
{
	// generate a new column reference
	CName name(pcr->Name().Pstr());
	CColRef *pcrNew = m_pcf->PcrCreate(pmdtype, name);

	if (fStoreMapping)
	{
#ifdef GPOS_DEBUG
		BOOL fResult =
#endif // GPOS_DEBUG
				m_phmulcr->FInsert(GPOS_NEW(m_pmp) ULONG(ulColId), pcrNew);

		GPOS_ASSERT(fResult);
	}

	return pcrNew;
}
//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::pdrgpcrOutput
//
//	@doc:
// 		Construct an array of new column references from the array of
//		DXL column descriptors
//
//---------------------------------------------------------------------------
DrgPcr *
CTranslatorDXLToExpr::Pdrgpcr
	(
	const DrgPdxlcd *pdrgpdxlcd
	)
{
	GPOS_ASSERT(NULL != pdrgpdxlcd);
	DrgPcr *pdrgpcrOutput = GPOS_NEW(m_pmp) DrgPcr(m_pmp);
	ULONG ulOutputCols = pdrgpdxlcd->UlLength();
	for (ULONG ul = 0; ul < ulOutputCols; ul++)
	{
		CDXLColDescr *pdxlcd = (*pdrgpdxlcd)[ul];
		IMDId *pmdid = pdxlcd->PmdidType();
		const IMDType *pmdtype = m_pmda->Pmdtype(pmdid);

		CName name(pdxlcd->Pmdname()->Pstr());
		// generate a new column reference
		CColRef *pcr = m_pcf->PcrCreate(pmdtype, name);
		pdrgpcrOutput->Append(pcr);
	}

	return pdrgpcrOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Pdrgpdxlcd
//
//	@doc:
// 		Construct an array of new column descriptors from the array of
//		DXL column descriptors
//
//---------------------------------------------------------------------------
DrgPcoldesc *
CTranslatorDXLToExpr::Pdrgpdxlcd
	(
	const DrgPdxlcd *pdrgpdxlcd
	)
{
	DrgPcoldesc *pdrgpcoldescOutput = GPOS_NEW(m_pmp) DrgPcoldesc(m_pmp);
	const ULONG ulColumns = pdrgpdxlcd->UlLength();

	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		CDXLColDescr *pdxlcd = (*pdrgpdxlcd)[ul];
		const IMDType *pmdtype = m_pmda->Pmdtype(pdxlcd->PmdidType());
		CName name(m_pmp, pdxlcd->Pmdname()->Pstr());

		const ULONG ulWidth = pdxlcd->UlWidth();

		CColumnDescriptor *pcoldesc = GPOS_NEW(m_pmp) CColumnDescriptor
													(
													m_pmp,
													pmdtype,
													name,
													ul + 1, // iAttno
													false, // FNullable
													ulWidth
													);
		pdrgpcoldescOutput->Append(pcoldesc);
	}

	return pdrgpcoldescOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::ConstructDXLColId2ColRefMapping
//
//	@doc:
// 		Construct the mapping between the DXL ColId and CColRef
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToExpr::ConstructDXLColId2ColRefMapping
	(
	const DrgPdxlcd *pdrgpdxlcd,
	const DrgPcr *pdrgpcr
	)
{
	GPOS_ASSERT(NULL != pdrgpdxlcd);
	GPOS_ASSERT(NULL != pdrgpcr);

	const ULONG ulColumns = pdrgpdxlcd->UlLength();
	GPOS_ASSERT(pdrgpcr->UlLength() == ulColumns);

	// construct the mapping between the DXL ColId and CColRef
	for (ULONG ul = 0; ul < ulColumns ; ul++)
	{
		CColRef *pcr = (*pdrgpcr)[ul];
		GPOS_ASSERT(NULL != pcr);

		const CDXLColDescr *pdxlcd = (*pdrgpdxlcd)[ul];
		GPOS_ASSERT(NULL != pdxlcd && !pdxlcd->FDropped());

		// copy key
		ULONG *pulKey = GPOS_NEW(m_pmp) ULONG(pdxlcd->UlID());
#ifdef GPOS_DEBUG
		BOOL fResult =
#endif // GPOS_DEBUG
		m_phmulcr->FInsert(pulKey, pcr);

		GPOS_ASSERT(fResult);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalSelect
//
//	@doc:
// 		Create a logical select expr from a DXL logical select
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalSelect
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);

	// translate the child dxl node
	CDXLNode *pdxlnChild = (*pdxln)[1];
	CExpression *pexprChild = PexprLogical(pdxlnChild);

	// translate scalar condition
	CDXLNode *pdxlnCond = (*pdxln)[0];
	CExpression *pexprCond = PexprScalar(pdxlnCond);
	CLogicalSelect *plgselect = GPOS_NEW(m_pmp) CLogicalSelect(m_pmp);
	CExpression *pexprSelect = GPOS_NEW(m_pmp) CExpression(m_pmp, plgselect, pexprChild,	pexprCond);

	return pexprSelect;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalProject
//
//	@doc:
// 		Create a logical project expr from a DXL logical project
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalProject
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);

	// translate the child dxl node
	CDXLNode *pdxlnChild = (*pdxln)[1];
	CExpression *pexprChild = PexprLogical(pdxlnChild);

	// translate the project list
	CDXLNode *pdxlnPrL = (*pdxln)[0];
	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());
	CExpression *pexprProjList = PexprScalarProjList(pdxlnPrL);

	CLogicalProject *popProject = GPOS_NEW(m_pmp) CLogicalProject(m_pmp);
	CExpression *pexprProject = GPOS_NEW(m_pmp) CExpression(m_pmp, popProject, pexprChild,	pexprProjList);

	return pexprProject;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalCTEAnchor
//
//	@doc:
// 		Create a logical CTE anchor expr from a DXL logical CTE anchor
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalCTEAnchor
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);
	CDXLLogicalCTEAnchor *pdxlopCTEAnchor = CDXLLogicalCTEAnchor::PdxlopConvert(pdxln->Pdxlop());
	ULONG ulCTEId = pdxlopCTEAnchor->UlId();

	CDXLNode *pdxlnCTEProducer = m_phmulpdxlnCTEProducer->PtLookup(&ulCTEId);
	GPOS_ASSERT(NULL != pdxlnCTEProducer);

	ULONG ulId = UlMapCTEId(ulCTEId);
	// mark that we are about to start processing this new CTE and keep track
	// of the previous one
	ULONG ulCTEPrevious = m_ulCTEId;
	m_ulCTEId = ulId;
	CExpression *pexprProducer = Pexpr(pdxlnCTEProducer);
	GPOS_ASSERT(NULL != pexprProducer);
	m_ulCTEId = ulCTEPrevious;
	
	CColRefSet *pcrsProducerOuter = CDrvdPropRelational::Pdprel(pexprProducer->PdpDerive())->PcrsOuter();
	if (0 < pcrsProducerOuter->CElements())
	{
		GPOS_RAISE
				(
				gpopt::ExmaGPOPT,
				gpopt::ExmiUnsupportedOp,
				GPOS_WSZ_LIT("CTE with outer references")
				);
	}

	COptCtxt::PoctxtFromTLS()->Pcteinfo()->AddCTEProducer(pexprProducer);
	pexprProducer->Release();

	// translate the child dxl node
	CExpression *pexprChild = PexprLogical((*pdxln)[0]);

	return GPOS_NEW(m_pmp) CExpression
						(
						m_pmp,
						GPOS_NEW(m_pmp) CLogicalCTEAnchor(m_pmp, ulId),
						pexprChild
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalCTEProducer
//
//	@doc:
// 		Create a logical CTE producer expr from a DXL logical CTE producer
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalCTEProducer
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);
	CDXLLogicalCTEProducer *pdxlopCTEProducer = CDXLLogicalCTEProducer::PdxlopConvert(pdxln->Pdxlop());
	ULONG ulId = UlMapCTEId(pdxlopCTEProducer->UlId());

	// translate the child dxl node
	CExpression *pexprChild = PexprLogical((*pdxln)[0]);

	// a column of the cte producer's child may be used in CTE producer output multiple times;
	// CTE consumer maintains a hash map between the cte producer columns to cte consumer columns.
	// To avoid losing mapping information of duplicate producer columns, we introduce a relabel
	// node (project element) for each duplicate entry of the producer column.

	DrgPexpr *pdrgpexprPrEl = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);
	CColRefSet *pcrsProducer = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	DrgPcr *pdrgpcr = GPOS_NEW(m_pmp) DrgPcr(m_pmp);

	DrgPul *pdrgpulCols = pdxlopCTEProducer->PdrgpulColIds();
	const ULONG ulLen = pdrgpulCols->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULONG *pulColId = (*pdrgpulCols)[ul];
		const CColRef *pcr = m_phmulcr->PtLookup(pulColId);
		GPOS_ASSERT(NULL != pcr);

		if (pcrsProducer->FMember(pcr))
		{
			// the column was previously used, so introduce a project node to relabel
			// the next use of the column reference
			CColRef *pcrNew = m_pcf->PcrCreate(pcr);
			CExpression *pexprPrEl = CUtils::PexprScalarProjectElement
												(
												m_pmp,
												pcrNew,
												GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarIdent(m_pmp, pcr))
												);
			pdrgpexprPrEl->Append(pexprPrEl);
			pcr = pcrNew;
		}

		pdrgpcr->Append(const_cast<CColRef*>(pcr));
		pcrsProducer->Include(pcr);
	}

	GPOS_ASSERT(ulLen == pdrgpcr->UlLength());

	if (0 < pdrgpexprPrEl->UlLength())
	{
		pdrgpexprPrEl->AddRef();
		CExpression *pexprPr = GPOS_NEW(m_pmp) CExpression
											(
											m_pmp,
											GPOS_NEW(m_pmp) CLogicalProject(m_pmp),
											pexprChild,
											GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarProjectList(m_pmp), pdrgpexprPrEl)
											);
		pexprChild = pexprPr;
	}

	// clean up
	pdrgpexprPrEl->Release();
	pcrsProducer->Release();

	return GPOS_NEW(m_pmp) CExpression
						(
						m_pmp,
						GPOS_NEW(m_pmp) CLogicalCTEProducer(m_pmp, ulId, pdrgpcr),
						pexprChild
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalCTEConsumer
//
//	@doc:
// 		Create a logical CTE consumer expr from a DXL logical CTE consumer
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalCTEConsumer
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);

	CDXLLogicalCTEConsumer *pdxlopCTEConsumer = CDXLLogicalCTEConsumer::PdxlopConvert(pdxln->Pdxlop());
	ULONG ulId = UlMapCTEId(pdxlopCTEConsumer->UlId());

	DrgPul *pdrgpulCols = pdxlopCTEConsumer->PdrgpulColIds();

	// create new col refs
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	CExpression *pexprProducer = pcteinfo->PexprCTEProducer(ulId);
	GPOS_ASSERT(NULL != pexprProducer);

	DrgPcr *pdrgpcrProducer = CLogicalCTEProducer::PopConvert(pexprProducer->Pop())->Pdrgpcr();
	DrgPcr *pdrgpcrConsumer = CUtils::PdrgpcrCopy(m_pmp, pdrgpcrProducer);

	// add new colrefs to mapping
	const ULONG ulCols = pdrgpcrConsumer->UlLength();
	GPOS_ASSERT(pdrgpulCols->UlLength() == ulCols);
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		ULONG *pulColId = GPOS_NEW(m_pmp) ULONG(*(*pdrgpulCols)[ul]);
		CColRef *pcr = (*pdrgpcrConsumer)[ul];

#ifdef GPOS_DEBUG
		BOOL fResult =
#endif // GPOS_DEBUG
		m_phmulcr->FInsert(pulColId, pcr);
		GPOS_ASSERT(fResult);
	}

	pcteinfo->IncrementConsumers(ulId, m_ulCTEId);
	return GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CLogicalCTEConsumer(m_pmp, ulId, pdrgpcrConsumer));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::UlMapCTEId
//
//	@doc:
// 		Return an new CTE id based on the CTE id used in DXL, since we may
//		introduce new CTEs during translation that did not exist in DXL
//
//---------------------------------------------------------------------------
ULONG
CTranslatorDXLToExpr::UlMapCTEId
	(
	const ULONG ulIdOld
	)
{
	ULONG *pulNewId =  m_phmululCTE->PtLookup(&ulIdOld);
	if (NULL == pulNewId)
	{
		pulNewId = GPOS_NEW(m_pmp) ULONG(COptCtxt::PoctxtFromTLS()->Pcteinfo()->UlNextId());

#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
		m_phmululCTE->FInsert(GPOS_NEW(m_pmp) ULONG(ulIdOld), pulNewId);
		GPOS_ASSERT(fInserted);
	}

	return *pulNewId;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalInsert
//
//	@doc:
// 		Create a logical DML on top of a project from a DXL logical insert
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalInsert
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);
	CDXLLogicalInsert *pdxlopInsert = CDXLLogicalInsert::PdxlopConvert(pdxln->Pdxlop());

	// translate the child dxl node
	CDXLNode *pdxlnChild = (*pdxln)[0];
	CExpression *pexprChild = PexprLogical(pdxlnChild);

	CTableDescriptor *ptabdesc = Ptabdesc(pdxlopInsert->Pdxltabdesc());

	DrgPul *pdrgpulSourceCols = pdxlopInsert->Pdrgpul();
	DrgPcr *pdrgpcr = CTranslatorDXLToExprUtils::Pdrgpcr(m_pmp, m_phmulcr, pdrgpulSourceCols);

	return GPOS_NEW(m_pmp) CExpression
						(
						m_pmp,
						GPOS_NEW(m_pmp) CLogicalInsert(m_pmp, ptabdesc, pdrgpcr),
						pexprChild
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalDelete
//
//	@doc:
// 		Create a logical DML on top of a project from a DXL logical delete
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalDelete
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);
	CDXLLogicalDelete *pdxlopDelete = CDXLLogicalDelete::PdxlopConvert(pdxln->Pdxlop());

	// translate the child dxl node
	CDXLNode *pdxlnChild = (*pdxln)[0];
	CExpression *pexprChild = PexprLogical(pdxlnChild);

	CTableDescriptor *ptabdesc = Ptabdesc(pdxlopDelete->Pdxltabdesc());

	ULONG ulCtid = pdxlopDelete->UlCtid();
	ULONG ulSegmentId = pdxlopDelete->UlSegmentId();

	CColRef *pcrCtid = PcrLookup(m_phmulcr, ulCtid);
	CColRef *pcrSegmentId = PcrLookup(m_phmulcr, ulSegmentId);

	DrgPul *pdrgpulCols = pdxlopDelete->PdrgpulDelete();
	DrgPcr *pdrgpcr = CTranslatorDXLToExprUtils::Pdrgpcr(m_pmp, m_phmulcr, pdrgpulCols);

	return GPOS_NEW(m_pmp) CExpression
						(
						m_pmp,
						GPOS_NEW(m_pmp) CLogicalDelete(m_pmp, ptabdesc, pdrgpcr, pcrCtid, pcrSegmentId),
						pexprChild
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalUpdate
//
//	@doc:
// 		Create a logical DML on top of a split from a DXL logical update
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalUpdate
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);
	CDXLLogicalUpdate *pdxlopUpdate = CDXLLogicalUpdate::PdxlopConvert(pdxln->Pdxlop());

	// translate the child dxl node
	CDXLNode *pdxlnChild = (*pdxln)[0];
	CExpression *pexprChild = PexprLogical(pdxlnChild);

	CTableDescriptor *ptabdesc = Ptabdesc(pdxlopUpdate->Pdxltabdesc());

	ULONG ulCtid = pdxlopUpdate->UlCtid();
	ULONG ulSegmentId = pdxlopUpdate->UlSegmentId();

	CColRef *pcrCtid = PcrLookup(m_phmulcr, ulCtid);
	CColRef *pcrSegmentId = PcrLookup(m_phmulcr, ulSegmentId);

	DrgPul *pdrgpulInsertCols = pdxlopUpdate->PdrgpulInsert();
	DrgPcr *pdrgpcrInsert = CTranslatorDXLToExprUtils::Pdrgpcr(m_pmp, m_phmulcr, pdrgpulInsertCols);

	DrgPul *pdrgpulDeleteCols = pdxlopUpdate->PdrgpulDelete();
	DrgPcr *pdrgpcrDelete = CTranslatorDXLToExprUtils::Pdrgpcr(m_pmp, m_phmulcr, pdrgpulDeleteCols);

	CColRef *pcrTupleOid = NULL;
	if (pdxlopUpdate->FPreserveOids())
	{
		ULONG ulTupleOid = pdxlopUpdate->UlTupleOid();
		pcrTupleOid = PcrLookup(m_phmulcr, ulTupleOid);
	}
	
	return GPOS_NEW(m_pmp) CExpression
						(
						m_pmp,
						GPOS_NEW(m_pmp) CLogicalUpdate(m_pmp, ptabdesc, pdrgpcrDelete, pdrgpcrInsert, pcrCtid, pcrSegmentId, pcrTupleOid),
						pexprChild
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalCTAS
//
//	@doc:
// 		Create a logical Insert from a logical DXL CTAS operator
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalCTAS
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);
	CDXLLogicalCTAS *pdxlopCTAS = CDXLLogicalCTAS::PdxlopConvert(pdxln->Pdxlop());

	// translate the child dxl node
	CDXLNode *pdxlnChild = (*pdxln)[0];
	CExpression *pexprChild = PexprLogical(pdxlnChild);

	RegisterMDRelationCtas(pdxlopCTAS);
	CTableDescriptor *ptabdesc = PtabdescFromCTAS(pdxlopCTAS);

	DrgPul *pdrgpulSourceCols = pdxlopCTAS->PdrgpulSource();
	DrgPcr *pdrgpcr = CTranslatorDXLToExprUtils::Pdrgpcr(m_pmp, m_phmulcr, pdrgpulSourceCols);

	return GPOS_NEW(m_pmp) CExpression
						(
						m_pmp,
						GPOS_NEW(m_pmp) CLogicalInsert(m_pmp, ptabdesc, pdrgpcr),
						pexprChild
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalGroupBy
//
//	@doc:
// 		Create a logical group by expr from a DXL logical group by aggregate
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalGroupBy
	(
	const CDXLNode *pdxln
	)
{
	// get children
	CDXLLogicalGroupBy *pdxlopGrpby = CDXLLogicalGroupBy::PdxlopConvert(pdxln->Pdxlop());
	CDXLNode *pdxlnPrL = (*pdxln)[0];
	CDXLNode *pdxlnChild = (*pdxln)[1];
	
	// translate the child dxl node
	CExpression *pexprChild = PexprLogical(pdxlnChild);

	// translate proj list
	CExpression *pexprProjList = PexprScalarProjList(pdxlnPrL);

	// translate grouping columns
	DrgPcr *pdrgpcrGroupingCols = CTranslatorDXLToExprUtils::Pdrgpcr(m_pmp, m_phmulcr, pdxlopGrpby->PdrgpulGroupingCols());
	
	if (0 != pexprProjList->UlArity())
	{
		GPOS_ASSERT(CUtils::FHasGlobalAggFunc(pexprProjList));
	}

	return GPOS_NEW(m_pmp) CExpression
						(
						m_pmp,
						GPOS_NEW(m_pmp) CLogicalGbAgg
									(
									m_pmp,
									pdrgpcrGroupingCols,
									COperator::EgbaggtypeGlobal /*egbaggtype*/
									),
						pexprChild,
						pexprProjList
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalLimit
//
//	@doc:
// 		Create a logical limit expr from a DXL logical limit node
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalLimit
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln && EdxlopLogicalLimit == pdxln->Pdxlop()->Edxlop());

	// get children
	CDXLNode *pdxlnSortColList = (*pdxln)[EdxllogicallimitIndexSortColList];
	CDXLNode *pdxlnCount = (*pdxln)[EdxllogicallimitIndexLimitCount];
	CDXLNode *pdxlnOffset = (*pdxln)[EdxllogicallimitIndexLimitOffset];
	CDXLNode *pdxlnChild = (*pdxln)[EdxllogicallimitIndexChildPlan];
	
	// translate count	
	CExpression *pexprLimitCount = NULL;
	BOOL fHasCount = false;
	if (1 == pdxlnCount->UlArity())
	{
		// translate limit count
		pexprLimitCount = Pexpr((*pdxlnCount)[0]);
		COperator *popCount = pexprLimitCount->Pop();
		BOOL fConst = (COperator::EopScalarConst == popCount->Eopid());
		if (!fConst ||
			(fConst && !CScalarConst::PopConvert(popCount)->Pdatum()->FNull()))
		{
			fHasCount = true;
		}
	}
	else
	{
		// no limit count is specified, manufacture a null count
		pexprLimitCount = CUtils::PexprScalarConstInt8(m_pmp, 0 /*iVal*/, true /*fNull*/);
	}
	
	// translate offset
	CExpression *pexprLimitOffset = NULL;

	if (1 == pdxlnOffset->UlArity())
	{
		pexprLimitOffset = Pexpr((*pdxlnOffset)[0]);
	}
	else
	{
		// manufacture an OFFSET 0
		pexprLimitOffset = CUtils::PexprScalarConstInt8(m_pmp, 0 /*iVal*/);
	}
	
	// translate limit child
	CExpression *pexprChild = PexprLogical(pdxlnChild);

	// translate sort col list
	COrderSpec *pos = Pos(pdxlnSortColList);
	
	BOOL fNonRemovable = CDXLLogicalLimit::PdxlopConvert(pdxln->Pdxlop())->FTopLimitUnderDML();
	CLogicalLimit *popLimit =
			GPOS_NEW(m_pmp) CLogicalLimit(m_pmp, pos, true /*fGlobal*/, fHasCount, fNonRemovable);
	return GPOS_NEW(m_pmp) CExpression(m_pmp, popLimit, pexprChild, pexprLimitOffset, pexprLimitCount);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalSeqPr
//
//	@doc:
// 		Create a logical sequence expr from a DXL logical window
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalSeqPr
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);
	
	CDXLLogicalWindow *pdxlopWindow = CDXLLogicalWindow::PdxlopConvert(pdxln->Pdxlop());
	
	CDXLNode *pdxlnWindowChild = (*pdxln)[1];
	CExpression *pexprWindowChild = PexprLogical(pdxlnWindowChild);

	// maintains the map between window specification position -> list of project elements
	// used to generate a cascade of window nodes
	HMUlPdrgpexpr *phmulpdrgpexpr = GPOS_NEW(m_pmp) HMUlPdrgpexpr(m_pmp);

	CDXLNode *pdxlnPrL = (*pdxln)[0];
	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());
	const ULONG ulArity = pdxlnPrL->UlArity();
	GPOS_ASSERT(0 < ulArity);

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnProjElem = (*pdxlnPrL)[ul];

		GPOS_ASSERT(NULL != pdxlnProjElem);
		GPOS_ASSERT(EdxlopScalarProjectElem == pdxlnProjElem->Pdxlop()->Edxlop() && 1 == pdxlnProjElem->UlArity());

		CDXLNode *pdxlnPrElChild = (*pdxlnProjElem)[0];
		// expect the project list to be normalized and expect to only find window functions and scalar identifiers
		GPOS_ASSERT(EdxlopScalarIdent == pdxlnPrElChild->Pdxlop()->Edxlop() || EdxlopScalarWindowRef == pdxlnPrElChild->Pdxlop()->Edxlop());
		CDXLScalarProjElem *pdxlopPrEl = CDXLScalarProjElem::PdxlopConvert(pdxlnProjElem->Pdxlop());
		
		if (EdxlopScalarWindowRef == pdxlnPrElChild->Pdxlop()->Edxlop())
		{
			// translate window function
			CDXLScalarWindowRef *pdxlopWindowRef = CDXLScalarWindowRef::PdxlopConvert(pdxlnPrElChild->Pdxlop());
			CExpression *pexprScWindowFunc = Pexpr(pdxlnPrElChild); 

			CScalar *popScalar = CScalar::PopConvert(pexprScWindowFunc->Pop());
			IMDId *pmdid = popScalar->PmdidType();
			const IMDType *pmdtype = m_pmda->Pmdtype(pmdid);

			CName name(pdxlopPrEl->PmdnameAlias()->Pstr());

			// generate a new column reference
			CColRef *pcr = m_pcf->PcrCreate(pmdtype, name);
			CScalarProjectElement *popScPrEl = GPOS_NEW(m_pmp) CScalarProjectElement(m_pmp, pcr);

			// store colid -> colref mapping
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
			m_phmulcr->FInsert(GPOS_NEW(m_pmp) ULONG(pdxlopPrEl->UlId()), pcr);
			GPOS_ASSERT(fInserted);

			// generate a project element
			CExpression *pexprProjElem = GPOS_NEW(m_pmp) CExpression(m_pmp, popScPrEl, pexprScWindowFunc);
			
			// add the created project element to the project list of the window node
			ULONG ulSpecPos = pdxlopWindowRef->UlWinSpecPos();			
			const DrgPexpr *pdrgpexpr = phmulpdrgpexpr->PtLookup(&ulSpecPos); 			
			if (NULL == pdrgpexpr)
			{
				DrgPexpr *pdrgpexprNew = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);
				pdrgpexprNew->Append(pexprProjElem);
#ifdef GPOS_DEBUG
			BOOL fInsert =
#endif
				phmulpdrgpexpr->FInsert(GPOS_NEW(m_pmp) ULONG(ulSpecPos), pdrgpexprNew);
				GPOS_ASSERT(fInsert);
			}
			else 
			{
				const_cast<DrgPexpr *>(pdrgpexpr)->Append(pexprProjElem);
			}
		}
	}

	// create the window operators (or when applicable a tree of window operators)
	CExpression *pexprLgSequence = NULL;
	HMIterUlPdrgpexpr hmiterulpdrgexpr(phmulpdrgpexpr);
	
	while (hmiterulpdrgexpr.FAdvance())
	{
		ULONG ulPos = *(hmiterulpdrgexpr.Pk());
		CDXLWindowSpec *pdxlws = pdxlopWindow->Pdxlws(ulPos);
		
		const DrgPexpr *pdrgpexpr = hmiterulpdrgexpr.Pt();
		GPOS_ASSERT(NULL != pdrgpexpr);
		CScalarProjectList *popPrL = GPOS_NEW(m_pmp) CScalarProjectList(m_pmp);
		CExpression *pexprProjList = GPOS_NEW(m_pmp) CExpression(m_pmp, popPrL, const_cast<DrgPexpr *>(pdrgpexpr));
		
		DrgPcr *pdrgpcr = PdrgpcrPartitionByCol(pdxlws->PdrgulPartColList());
		CDistributionSpec *pds = NULL;
		if (0 < pdrgpcr->UlLength())
		{
			DrgPexpr *pdrgpexprScalarIdents = CUtils::PdrgpexprScalarIdents(m_pmp, pdrgpcr);
			pds = GPOS_NEW(m_pmp) CDistributionSpecHashed(pdrgpexprScalarIdents, true /* fNullsCollocated */);
		}
		else
		{
			// if no partition-by columns, window functions need gathered input
			pds = GPOS_NEW(m_pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
		}
		pdrgpcr->Release();
		
		DrgPwf *pdrgpwf = GPOS_NEW(m_pmp) DrgPwf(m_pmp);
		CWindowFrame *pwf = NULL;
		if (NULL != pdxlws->Pdxlwf()) 
		{
			pwf = Pwf(pdxlws->Pdxlwf());
		}
		else
		{
			// create an empty frame
			pwf = const_cast<CWindowFrame *>(CWindowFrame::PwfEmpty());
			pwf->AddRef();
		}
		pdrgpwf->Append(pwf);
		
		DrgPos *pdrgpos = GPOS_NEW(m_pmp) DrgPos(m_pmp);
		if (NULL != pdxlws->PdxlnSortColList()) 
		{
			COrderSpec *pos = Pos(pdxlws->PdxlnSortColList());
			pdrgpos->Append(pos);
		}
		else
		{
			pdrgpos->Append(GPOS_NEW(m_pmp) COrderSpec(m_pmp));
		}
		
		CLogicalSequenceProject *popLgSequence = GPOS_NEW(m_pmp) CLogicalSequenceProject(m_pmp, pds, pdrgpos, pdrgpwf);
		pexprLgSequence = GPOS_NEW(m_pmp) CExpression(m_pmp, popLgSequence, pexprWindowChild, pexprProjList);
		pexprWindowChild = pexprLgSequence;
	}
	
	GPOS_ASSERT(NULL != pexprLgSequence);
	
	// clean up
	phmulpdrgpexpr->Release();

	return pexprLgSequence;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PdrgpcrPartitionByCol
//
//	@doc:
// 		Create the array of column reference used in the partition by column
//		list of a window specification
//
//---------------------------------------------------------------------------
DrgPcr *
CTranslatorDXLToExpr::PdrgpcrPartitionByCol
	(
	const DrgPul *pdrgpulPartCol
	)
{
	const ULONG ulSize = pdrgpulPartCol->UlLength();
	DrgPcr *pdrgpcr = GPOS_NEW(m_pmp) DrgPcr(m_pmp);
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		const ULONG *pulColId = (*pdrgpulPartCol)[ul];

		// get its column reference from the hash map
		CColRef *pcr =  PcrLookup(m_phmulcr, *pulColId);
		pdrgpcr->Append(pcr);
	}
	
	return pdrgpcr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Pwf
//
//	@doc:
// 		Create a window frame from a DXL window frame node
//
//---------------------------------------------------------------------------
CWindowFrame *
CTranslatorDXLToExpr::Pwf
	(
	const CDXLWindowFrame *pdxlwf
	)
{
	CDXLNode *pdxlnTrail = pdxlwf->PdxlnTrailing();
	CDXLNode *pdxlnLead = pdxlwf->PdxlnLeading();

	CWindowFrame::EFrameBoundary efbLead = Efb(CDXLScalarWindowFrameEdge::PdxlopConvert(pdxlnLead->Pdxlop())->Edxlfb());
	CWindowFrame::EFrameBoundary efbTrail = Efb(CDXLScalarWindowFrameEdge::PdxlopConvert(pdxlnTrail->Pdxlop())->Edxlfb());

	CExpression *pexprTrail = NULL;
	if (0 != pdxlnTrail->UlArity())
	{
		pexprTrail = Pexpr((*pdxlnTrail)[0]);
	}

	CExpression *pexprLead = NULL;
	if (0 != pdxlnLead->UlArity())
	{
		pexprLead = Pexpr((*pdxlnLead)[0]);
	}

	CWindowFrame::EFrameExclusionStrategy efes = Efes(pdxlwf->Edxlfes());
	CWindowFrame::EFrameSpec efs = CWindowFrame::EfsRows;
	if (EdxlfsRange == pdxlwf->Edxlfs())
	{
		efs = CWindowFrame::EfsRange;
	}
	
	CWindowFrame *pwf = GPOS_NEW(m_pmp) CWindowFrame(m_pmp, efs, efbLead, efbTrail, pexprLead, pexprTrail, efes);
	
	return pwf;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Efb
//
//	@doc:
//		Return the window frame boundary
//
//---------------------------------------------------------------------------
CWindowFrame::EFrameBoundary
CTranslatorDXLToExpr::Efb
	(
	EdxlFrameBoundary edxlfb
	)
	const
{
	ULONG rgrgulMapping[][2] =
	{
		{EdxlfbUnboundedPreceding, CWindowFrame::EfbUnboundedPreceding},
		{EdxlfbBoundedPreceding, CWindowFrame::EfbBoundedPreceding},
		{EdxlfbCurrentRow, CWindowFrame::EfbCurrentRow},
		{EdxlfbUnboundedFollowing, CWindowFrame::EfbUnboundedFollowing},
		{EdxlfbBoundedFollowing, CWindowFrame::EfbBoundedFollowing},
		{EdxlfbDelayedBoundedPreceding, CWindowFrame::EfbDelayedBoundedPreceding},
		{EdxlfbDelayedBoundedFollowing, CWindowFrame::EfbDelayedBoundedFollowing}
	};

#ifdef GPOS_DEBUG
	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	GPOS_ASSERT(ulArity > (ULONG) edxlfb  && "Invalid window frame boundary");
#endif
	CWindowFrame::EFrameBoundary efb = (CWindowFrame::EFrameBoundary)rgrgulMapping[(ULONG) edxlfb][1];
	
	return efb;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Efes
//
//	@doc:
//		Return the window frame exclusion strategy
//
//---------------------------------------------------------------------------
CWindowFrame::EFrameExclusionStrategy
CTranslatorDXLToExpr::Efes
(
 EdxlFrameExclusionStrategy edxlfeb
 )
const
{
	ULONG rgrgulMapping[][2] =
	{
		{EdxlfesNone, CWindowFrame::EfesNone},
		{EdxlfesNulls, CWindowFrame::EfesNulls},
		{EdxlfesCurrentRow, CWindowFrame::EfesCurrentRow},
		{EdxlfesGroup, CWindowFrame::EfseMatchingOthers},
		{EdxlfesTies, CWindowFrame::EfesTies}
	};

#ifdef GPOS_DEBUG
	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	GPOS_ASSERT(ulArity > (ULONG) edxlfeb  && "Invalid window frame exclusion strategy");
#endif
	CWindowFrame::EFrameExclusionStrategy efeb = (CWindowFrame::EFrameExclusionStrategy)rgrgulMapping[(ULONG) edxlfeb][1];

	return efeb;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalJoin
//
//	@doc:
// 		Create a logical join expr from a DXL logical join
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalJoin
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);

	CDXLLogicalJoin *pdxlopJoin = CDXLLogicalJoin::PdxlopConvert(pdxln->Pdxlop());
	EdxlJoinType edxljt = pdxlopJoin->Edxltype();

	if (EdxljtRight == edxljt)
	{
		return PexprRightOuterJoin(pdxln);
	}

	if (EdxljtInner != edxljt && EdxljtLeft != edxljt && EdxljtFull != edxljt)
	{
		GPOS_RAISE
				(
				gpopt::ExmaGPOPT,
				gpopt::ExmiUnsupportedOp,
				CDXLOperator::PstrJoinTypeName(pdxlopJoin->Edxltype())->Wsz()
				);
	}

	DrgPexpr *pdrgpexprChildren = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);

	const ULONG ulChildCount = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulChildCount-1; ++ul)
	{
		// get the next child dxl node and then translate it into an Expr
		CDXLNode *pdxlnNxtChild = (*pdxln)[ul];

		CExpression *pexprNxtChild = PexprLogical(pdxlnNxtChild);
		pdrgpexprChildren->Append(pexprNxtChild);
	}

	// get the scalar condition and then translate it
	CDXLNode *pdxlnCond = (*pdxln)[ulChildCount-1];
	CExpression *pexprCond = PexprScalar(pdxlnCond);
	pdrgpexprChildren->Append(pexprCond);

	return CUtils::PexprLogicalJoin(m_pmp, edxljt, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprRightOuterJoin
//
//	@doc:
// 		Translate a DXL right outer join. The expression A ROJ B is translated
//		to: B LOJ A
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprRightOuterJoin
	(
	const CDXLNode *pdxln
	)
{
#ifdef GPOS_DEBUG
	CDXLLogicalJoin *pdxlopJoin = CDXLLogicalJoin::PdxlopConvert(pdxln->Pdxlop());
	const ULONG ulChildCount = pdxln->UlArity();
#endif //GPOS_DEBUG
	GPOS_ASSERT(EdxljtRight == pdxlopJoin->Edxltype() && 3 == ulChildCount);

	DrgPexpr *pdrgpexprChildren = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);
	pdrgpexprChildren->Append(PexprLogical((*pdxln)[1]));
	pdrgpexprChildren->Append(PexprLogical((*pdxln)[0]));
	pdrgpexprChildren->Append(PexprScalar((*pdxln)[2]));

	return CUtils::PexprLogicalJoin(m_pmp, EdxljtLeft, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Ptabdesc
//
//	@doc:
//		Construct a table descriptor from DXL table descriptor 
//
//---------------------------------------------------------------------------
CTableDescriptor *
CTranslatorDXLToExpr::Ptabdesc
	(
	CDXLTableDescr *pdxltabdesc
	)
{
	CWStringConst strName(m_pmp, pdxltabdesc->Pmdname()->Pstr()->Wsz());

	IMDId *pmdid = pdxltabdesc->Pmdid();

	// get the relation information from the cache
	const IMDRelation *pmdrel = m_pmda->Pmdrel(pmdid);

	// construct mappings for columns that are not dropped
	HMIUl *phmiulAttnoColMapping = GPOS_NEW(m_pmp) HMIUl(m_pmp);
	HMUlUl *phmululColMapping = GPOS_NEW(m_pmp) HMUlUl(m_pmp);
	
	const ULONG ulAllColumns = pmdrel->UlColumns();
	ULONG ulPosNonDropped = 0;
	for (ULONG ulPos = 0; ulPos < ulAllColumns; ulPos++)
	{
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ulPos);
		if (pmdcol->FDropped())
		{
			continue;
		}
		(void) phmiulAttnoColMapping->FInsert(GPOS_NEW(m_pmp) INT(pmdcol->IAttno()), GPOS_NEW(m_pmp) ULONG(ulPosNonDropped));
		(void) phmululColMapping->FInsert(GPOS_NEW(m_pmp) ULONG(ulPos), GPOS_NEW(m_pmp) ULONG(ulPosNonDropped));

		ulPosNonDropped++;
	}
	
	// get distribution policy
	IMDRelation::Ereldistrpolicy ereldistrpolicy = pmdrel->Ereldistribution();

	// get storage type
	IMDRelation::Erelstoragetype erelstorage = pmdrel->Erelstorage();

	pmdid->AddRef();
	CTableDescriptor *ptabdesc = GPOS_NEW(m_pmp) CTableDescriptor
						(
						m_pmp,
						pmdid,
						CName(m_pmp, &strName),
						pmdrel->FConvertHashToRandom(),
						ereldistrpolicy,
						erelstorage,
						pdxltabdesc->UlExecuteAsUser()
						);

	const ULONG ulColumns = pdxltabdesc->UlArity();
	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		const CDXLColDescr *pdxlcoldesc = pdxltabdesc->Pdxlcd(ul);
		INT iAttno = pdxlcoldesc->IAttno();

		ULONG ulPos = pmdrel->UlPosFromAttno(pdxlcoldesc->IAttno());
		const IMDColumn *pmdcolNext = pmdrel->Pmdcol(ulPos);

		BOOL fNullable = pmdcolNext->FNullable();

		GPOS_ASSERT(pdxlcoldesc->PmdidType()->FValid());
		const IMDType *pmdtype = m_pmda->Pmdtype(pdxlcoldesc->PmdidType());

		GPOS_ASSERT(NULL != pdxlcoldesc->Pmdname()->Pstr()->Wsz());
		CWStringConst strColName(m_pmp, pdxlcoldesc->Pmdname()->Pstr()->Wsz());

		const ULONG ulWidth = pdxlcoldesc->UlWidth();
		CColumnDescriptor *pcoldesc = GPOS_NEW(m_pmp) CColumnDescriptor
													(
													m_pmp,
													pmdtype,
													CName(m_pmp, &strColName),
													iAttno,
													fNullable,
													ulWidth
													);

		ptabdesc->AddColumn(pcoldesc);
	}
	
	if (IMDRelation::EreldistrHash == ereldistrpolicy)
	{
		AddDistributionColumns(ptabdesc, pmdrel, phmiulAttnoColMapping);
	}

	if (pmdrel->FPartitioned())
	{
		const ULONG ulPartCols = pmdrel->UlPartColumns();
		// compute partition columns for table descriptor
		for (ULONG ul = 0; ul < ulPartCols; ul++)
		{
			const IMDColumn *pmdcol = pmdrel->PmdcolPartColumn(ul);
			INT iAttNo = pmdcol->IAttno();
			ULONG *pulPos = phmiulAttnoColMapping->PtLookup(&iAttNo);
			GPOS_ASSERT(NULL != pulPos);
			ptabdesc->AddPartitionColumn(*pulPos);
		}
	}
	
	// populate key sets
	CTranslatorDXLToExprUtils::AddKeySets(m_pmp, ptabdesc, pmdrel, phmululColMapping);
	
	phmiulAttnoColMapping->Release();
	phmululColMapping->Release();

	return ptabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::RegisterMDRelationCtas
//
//	@doc:
//		Register the MD relation entry for the given CTAS operator
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToExpr::RegisterMDRelationCtas
	(
	CDXLLogicalCTAS *pdxlopCTAS
	)
{
	GPOS_ASSERT(NULL != pdxlopCTAS);
	
	pdxlopCTAS->Pmdid()->AddRef();
	
	if (NULL != pdxlopCTAS->PdrgpulDistr())
	{
		pdxlopCTAS->PdrgpulDistr()->AddRef();
	}
	pdxlopCTAS->Pdxlctasopt()->AddRef();
	
	DrgPmdcol *pdrgpmdcol = GPOS_NEW(m_pmp) DrgPmdcol(m_pmp);
	DrgPdxlcd *pdrgpdxlcd = pdxlopCTAS->Pdrgpdxlcd();
	const ULONG ulLength = pdrgpdxlcd->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		CDXLColDescr *pdxlcd = (*pdrgpdxlcd)[ul];
		pdxlcd->PmdidType()->AddRef();
		
		CMDColumn *pmdcol = GPOS_NEW(m_pmp) CMDColumn
				(
				GPOS_NEW(m_pmp) CMDName(m_pmp, pdxlcd->Pmdname()->Pstr()),
				pdxlcd->IAttno(),
				pdxlcd->PmdidType(),
				true, // fNullable,
				pdxlcd->FDropped(),
				NULL, // pdxlnDefaultValue,
				pdxlcd->UlWidth()
				);
		pdrgpmdcol->Append(pmdcol);
	}
	
	CMDName *pmdnameSchema = NULL;
	if (NULL != pdxlopCTAS->PmdnameSchema())
	{
		pmdnameSchema = GPOS_NEW(m_pmp) CMDName(m_pmp, pdxlopCTAS->PmdnameSchema()->Pstr());
	}
	
	DrgPi * pdrgpiVarTypeMod = pdxlopCTAS->PdrgpiVarTypeMod();
	pdrgpiVarTypeMod->AddRef();
	CMDRelationCtasGPDB *pmdrel = GPOS_NEW(m_pmp) CMDRelationCtasGPDB
			(
			m_pmp,
			pdxlopCTAS->Pmdid(),
			pmdnameSchema,
			GPOS_NEW(m_pmp) CMDName(m_pmp, pdxlopCTAS->Pmdname()->Pstr()),
			pdxlopCTAS->FTemporary(),
			pdxlopCTAS->FHasOids(),
			pdxlopCTAS->Erelstorage(),
			pdxlopCTAS->Ereldistrpolicy(),
			pdrgpmdcol,
			pdxlopCTAS->PdrgpulDistr(),
			GPOS_NEW(m_pmp) DrgPdrgPul(m_pmp), // pdrgpdrgpulKeys,
			pdxlopCTAS->Pdxlctasopt(),
			pdrgpiVarTypeMod
			);
	
	DrgPimdobj *pdrgpmdobj = GPOS_NEW(m_pmp) DrgPimdobj(m_pmp);
	pdrgpmdobj->Append(pmdrel);
	CMDProviderMemory *pmdp = GPOS_NEW(m_pmp) CMDProviderMemory(m_pmp, pdrgpmdobj);
	m_pmda->RegisterProvider(pdxlopCTAS->Pmdid()->Sysid(), pmdp);
	
	// cleanup
	pdrgpmdobj->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PtabdescFromCTAS
//
//	@doc:
//		Construct a table descriptor for a CTAS operator
//
//---------------------------------------------------------------------------
CTableDescriptor *
CTranslatorDXLToExpr::PtabdescFromCTAS
	(
	CDXLLogicalCTAS *pdxlopCTAS
	)
{
	CWStringConst strName(m_pmp, pdxlopCTAS->Pmdname()->Pstr()->Wsz());

	IMDId *pmdid = pdxlopCTAS->Pmdid();

	// get the relation information from the cache
	const IMDRelation *pmdrel = m_pmda->Pmdrel(pmdid);

	// construct mappings for columns that are not dropped
	HMIUl *phmiulAttnoColMapping = GPOS_NEW(m_pmp) HMIUl(m_pmp);
	HMUlUl *phmululColMapping = GPOS_NEW(m_pmp) HMUlUl(m_pmp);
	
	const ULONG ulAllColumns = pmdrel->UlColumns();
	ULONG ulPosNonDropped = 0;
	for (ULONG ulPos = 0; ulPos < ulAllColumns; ulPos++)
	{
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ulPos);
		if (pmdcol->FDropped())
		{
			continue;
		}
		(void) phmiulAttnoColMapping->FInsert(GPOS_NEW(m_pmp) INT(pmdcol->IAttno()), GPOS_NEW(m_pmp) ULONG(ulPosNonDropped));
		(void) phmululColMapping->FInsert(GPOS_NEW(m_pmp) ULONG(ulPos), GPOS_NEW(m_pmp) ULONG(ulPosNonDropped));

		ulPosNonDropped++;
	}
	
	// get distribution policy
	IMDRelation::Ereldistrpolicy ereldistrpolicy = pmdrel->Ereldistribution();

	// get storage type
	IMDRelation::Erelstoragetype erelstorage = pmdrel->Erelstorage();

	pmdid->AddRef();
	CTableDescriptor *ptabdesc = GPOS_NEW(m_pmp) CTableDescriptor
						(
						m_pmp,
						pmdid,
						CName(m_pmp, &strName),
						pmdrel->FConvertHashToRandom(),
						ereldistrpolicy,
						erelstorage,
						0 // TODO:  - Mar 5, 2014; ulExecuteAsUser
						);

	// populate column information from the dxl table descriptor
	DrgPdxlcd *pdrgpdxlcd = pdxlopCTAS->Pdrgpdxlcd();
	const ULONG ulColumns = pdrgpdxlcd->UlLength();
	for (ULONG ul = 0; ul < ulColumns; ul++)
	{
		BOOL fNullable = false;
		if (ul < pmdrel->UlColumns())
		{
			fNullable = pmdrel->Pmdcol(ul)->FNullable();
		}

		const CDXLColDescr *pdxlcoldesc = (*pdrgpdxlcd)[ul];

		GPOS_ASSERT(pdxlcoldesc->PmdidType()->FValid());
		const IMDType *pmdtype = m_pmda->Pmdtype(pdxlcoldesc->PmdidType());

		GPOS_ASSERT(NULL != pdxlcoldesc->Pmdname()->Pstr()->Wsz());
		CWStringConst strColName(m_pmp, pdxlcoldesc->Pmdname()->Pstr()->Wsz());

		INT iAttNo = pdxlcoldesc->IAttno();

		const ULONG ulWidth = pdxlcoldesc->UlWidth();
		CColumnDescriptor *pcoldesc = GPOS_NEW(m_pmp) CColumnDescriptor
													(
													m_pmp,
													pmdtype,
													CName(m_pmp, &strColName),
													iAttNo,
													fNullable,
													ulWidth
													);

		ptabdesc->AddColumn(pcoldesc);
	}
	
	if (IMDRelation::EreldistrHash == ereldistrpolicy)
	{
		AddDistributionColumns(ptabdesc, pmdrel, phmiulAttnoColMapping);
	}

	GPOS_ASSERT(!pmdrel->FPartitioned());

	phmiulAttnoColMapping->Release();
	phmululColMapping->Release();

	return ptabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarSubqueryExistential
//
//	@doc:
// 		Translate existential subquery
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarSubqueryExistential
	(
	Edxlopid edxlopid,
	CDXLNode *pdxlnLogicalChild
	)
{
	GPOS_ASSERT(EdxlopScalarSubqueryExists == edxlopid || EdxlopScalarSubqueryNotExists == edxlopid);
	GPOS_ASSERT(NULL != pdxlnLogicalChild);

	CExpression *pexprLogicalChild = Pexpr(pdxlnLogicalChild);
	GPOS_ASSERT(NULL != pexprLogicalChild);

	CScalar *popScalarSubquery = NULL;
	if (EdxlopScalarSubqueryExists == edxlopid)
	{
		popScalarSubquery = GPOS_NEW(m_pmp) CScalarSubqueryExists(m_pmp);
	}
	else
	{
		popScalarSubquery = GPOS_NEW(m_pmp) CScalarSubqueryNotExists(m_pmp);
	}
	
	return GPOS_NEW(m_pmp) CExpression(m_pmp, popScalarSubquery, pexprLogicalChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprLogicalConstTableGet
//
//	@doc:
// 		Create a logical const table get expression from the corresponding
//		DXL node
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprLogicalConstTableGet
	(
	const CDXLNode *pdxlnConstTable
	)
{
	CDXLLogicalConstTable *pdxlopConstTable = CDXLLogicalConstTable::PdxlopConvert(pdxlnConstTable->Pdxlop());

	// translate the column descriptors
	DrgPcoldesc *pdrgpcoldesc = Pdrgpdxlcd(pdxlopConstTable->Pdrgpdxlcd());

	DrgPdrgPdatum *pdrgpdrgpdatum = GPOS_NEW(m_pmp) DrgPdrgPdatum(m_pmp);
	
	// translate values
	const ULONG ulValues = pdxlopConstTable->UlTupleCount();
	for (ULONG ul = 0; ul < ulValues; ul++)
	{
		const DrgPdxldatum *pdrgpdxldatum = pdxlopConstTable->PrgPdxldatumConstTuple(ul);
		DrgPdatum *pdrgpdatum = CTranslatorDXLToExprUtils::Pdrgpdatum(m_pmp, m_pmda, pdrgpdxldatum);
		pdrgpdrgpdatum->Append(pdrgpdatum);
	}

	// create a logical const table get operator
	CLogicalConstTableGet *popConstTableGet = GPOS_NEW(m_pmp) CLogicalConstTableGet
															(
															m_pmp,
															pdrgpcoldesc,
															pdrgpdrgpdatum
															);

	// construct the mapping between the DXL ColId and CColRef
	ConstructDXLColId2ColRefMapping(pdxlopConstTable->Pdrgpdxlcd(), popConstTableGet->PdrgpcrOutput());

	return GPOS_NEW(m_pmp) CExpression(m_pmp, popConstTableGet);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarSubqueryQuantified
//
//	@doc:
// 		Helper for creating quantified subquery
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarSubqueryQuantified
	(
	Edxlopid edxlopid,
	IMDId *pmdidScalarOp,
	const CWStringConst *pstr,
	ULONG ulColId,
	CDXLNode *pdxlnLogicalChild,
	CDXLNode *pdxlnScalarChild
	)
{
	GPOS_ASSERT(EdxlopScalarSubqueryAny == edxlopid || EdxlopScalarSubqueryAll == edxlopid);
	GPOS_ASSERT(NULL != pstr);
	GPOS_ASSERT(NULL != pdxlnLogicalChild);
	GPOS_ASSERT(NULL != pdxlnScalarChild);

	// translate children

	CExpression *pexprLogicalChild = Pexpr(pdxlnLogicalChild);
	CExpression *pexprScalarChild = Pexpr(pdxlnScalarChild);

	// get colref for subquery colid
	const CColRef *pcr = PcrLookup(m_phmulcr, ulColId);

	CScalar *popScalarSubquery = NULL;
	if (EdxlopScalarSubqueryAny == edxlopid)
	{
		popScalarSubquery = GPOS_NEW(m_pmp) CScalarSubqueryAny
								(
								m_pmp,
								pmdidScalarOp,
								GPOS_NEW(m_pmp) CWStringConst(m_pmp, pstr->Wsz()),
								pcr
								);
	}
	else
	{
		popScalarSubquery = GPOS_NEW(m_pmp) CScalarSubqueryAll
								(
								m_pmp,
								pmdidScalarOp,
								GPOS_NEW(m_pmp) CWStringConst(m_pmp, pstr->Wsz()),
								pcr
								);
	}

	// create a scalar subquery any expression with the relational expression as
	// first child and the scalar expression as second child
	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, popScalarSubquery, pexprLogicalChild, pexprScalarChild);

	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarSubqueryQuantified
//
//	@doc:
// 		Create a quantified subquery from a DXL node
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarSubqueryQuantified
	(
	const CDXLNode *pdxlnSubquery
	)
{
	GPOS_ASSERT(NULL != pdxlnSubquery);
	
	CDXLScalar *pdxlop = dynamic_cast<CDXLScalar *>(pdxlnSubquery->Pdxlop());
	GPOS_ASSERT(NULL != pdxlop);

	CDXLScalarSubqueryQuantified *pdxlopSubqueryQuantified = CDXLScalarSubqueryQuantified::PdxlopConvert(pdxlnSubquery->Pdxlop());
	GPOS_ASSERT(NULL != pdxlopSubqueryQuantified);

	IMDId *pmdid = pdxlopSubqueryQuantified->PmdidScalarOp();
	pmdid->AddRef();
	return PexprScalarSubqueryQuantified
		(
		pdxlop->Edxlop(),
		pmdid,
		pdxlopSubqueryQuantified->PmdnameScalarOp()->Pstr(),
		pdxlopSubqueryQuantified->UlColId(),
		(*pdxlnSubquery)[CDXLScalarSubqueryQuantified::EdxlsqquantifiedIndexRelational],
		(*pdxlnSubquery)[CDXLScalarSubqueryQuantified::EdxlsqquantifiedIndexScalar]
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalar
//
//	@doc:
// 		Create a logical select expr from a DXL logical select
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalar
	(
	const CDXLNode *pdxlnOp
	)
{
	GPOS_ASSERT(NULL != pdxlnOp);
	CDXLOperator *pdxlop = pdxlnOp->Pdxlop();
	ULONG ulOpId =  (ULONG) pdxlop->Edxlop();

	if (EdxlopScalarSubqueryExists == ulOpId || EdxlopScalarSubqueryNotExists == ulOpId)
	{
		return PexprScalarSubqueryExistential(pdxlnOp->Pdxlop()->Edxlop(), (*pdxlnOp)[0]);
	}
	
	PfPexpr pf = m_rgpfTranslators[ulOpId];

	if (NULL == pf)
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, pdxlop->PstrOpName()->Wsz());
	}
	
	return (this->* pf)(pdxlnOp);	
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprCollapseNot
//
//	@doc:
// 		Collapse a NOT node by looking at its child.
//		Return NULL if it is not collapsible.
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprCollapseNot
	(
	const CDXLNode *pdxlnNotExpr
	)
{
	GPOS_ASSERT(NULL != pdxlnNotExpr);
	GPOS_ASSERT(CTranslatorDXLToExprUtils::FScalarBool(pdxlnNotExpr, Edxlnot));

	CDXLNode *pdxlnNotChild = (*pdxlnNotExpr)[0];

	if (CTranslatorDXLToExprUtils::FScalarBool(pdxlnNotChild, Edxlnot))
	{
		// two cascaded NOT nodes cancel each other
		return Pexpr((*pdxlnNotChild)[0]);
	}

	Edxlopid edxlopid = pdxlnNotChild->Pdxlop()->Edxlop();
	if (EdxlopScalarSubqueryExists == edxlopid || EdxlopScalarSubqueryNotExists == edxlopid)
	{
		// NOT followed by EXISTS/NOTEXISTS is translated as NOTEXISTS/EXISTS
		Edxlopid edxlopidNew = (EdxlopScalarSubqueryExists == edxlopid)? EdxlopScalarSubqueryNotExists : EdxlopScalarSubqueryExists;

		return PexprScalarSubqueryExistential(edxlopidNew, (*pdxlnNotChild)[0]);
	}

	if (EdxlopScalarSubqueryAny == edxlopid || EdxlopScalarSubqueryAll == edxlopid)
	{
		// NOT followed by ANY/ALL<op> is translated as ALL/ANY<inverse_op>
		CDXLScalarSubqueryQuantified *pdxlopSubqueryQuantified = CDXLScalarSubqueryQuantified::PdxlopConvert(pdxlnNotChild->Pdxlop());
		Edxlopid edxlopidNew = (EdxlopScalarSubqueryAny == edxlopid)? EdxlopScalarSubqueryAll : EdxlopScalarSubqueryAny;

		// get mdid and name of the inverse of the comparison operator used by quantified subquery
		IMDId *pmdidOp = pdxlopSubqueryQuantified->PmdidScalarOp();
		IMDId *pmdidInverseOp = m_pmda->Pmdscop(pmdidOp)->PmdidOpInverse();

		// if inverse operator cannot be found in metadata, the optimizer won't collapse NOT node
		if (NULL == pmdidInverseOp)
		{
			return NULL;
		}

		const CWStringConst *pstrInverseOp = m_pmda->Pmdscop(pmdidInverseOp)->Mdname().Pstr();
		
		pmdidInverseOp->AddRef();
		return PexprScalarSubqueryQuantified
				(
				edxlopidNew,
				pmdidInverseOp,
				pstrInverseOp,
				pdxlopSubqueryQuantified->UlColId(),
				(*pdxlnNotChild)[CDXLScalarSubqueryQuantified::EdxlsqquantifiedIndexRelational],
				(*pdxlnNotChild)[CDXLScalarSubqueryQuantified::EdxlsqquantifiedIndexScalar]
				);
	}

	// collapsing NOT node failed
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarBoolOp
//
//	@doc:
// 		Create a scalar logical op representation in the optimizer 
//		from a DXL scalar boolean expr
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarBoolOp
	(
	const CDXLNode *pdxlnBoolExpr
	)
{
	GPOS_ASSERT(NULL != pdxlnBoolExpr);

	EdxlBoolExprType edxlbooltype = CDXLScalarBoolExpr::PdxlopConvert(pdxlnBoolExpr->Pdxlop())->EdxlBoolType();

	GPOS_ASSERT( (edxlbooltype == Edxlnot) || (edxlbooltype == Edxlor) || (edxlbooltype == Edxland));
	GPOS_ASSERT_IMP(Edxlnot == edxlbooltype, 1 == pdxlnBoolExpr->UlArity());
	
	if (Edxlnot == edxlbooltype)
	{
		// attempt collapsing NOT node
		CExpression *pexprResult = PexprCollapseNot(pdxlnBoolExpr);
		if (NULL != pexprResult)
		{
			return pexprResult;
		}
	}

	CScalarBoolOp::EBoolOperator eboolop = CTranslatorDXLToExprUtils::EBoolOperator(edxlbooltype);

	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxlnBoolExpr);

	return CUtils::PexprScalarBoolOp(m_pmp, eboolop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarOp
//
//	@doc:
// 		Create a scalar operation from a DXL scalar op expr
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarOp
	(
	const CDXLNode *pdxlnOpExpr
	)
{
	// TODO: Aug 22 2011; In GPDB the opexpr can only have two children. However, in other
	// databases this may not be the case
	GPOS_ASSERT(NULL != pdxlnOpExpr && (1 == pdxlnOpExpr->UlArity() || (2 == pdxlnOpExpr->UlArity())) );

	CDXLScalarOpExpr *pdxlop = CDXLScalarOpExpr::PdxlopConvert(pdxlnOpExpr->Pdxlop());

	DrgPexpr *pdrgpexprArgs = PdrgpexprChildren(pdxlnOpExpr);

	IMDId *pmdid = pdxlop->Pmdid();
	pmdid->AddRef();
	
	IMDId *pmdidReturnType = pdxlop->PmdidReturnType(); 
	if (NULL != pmdidReturnType)
	{
		pmdidReturnType->AddRef();
	}
	CScalarOp *pscop = GPOS_NEW(m_pmp) CScalarOp
										(
										m_pmp,
										pmdid,
										pmdidReturnType,
										GPOS_NEW(m_pmp) CWStringConst(m_pmp, pdxlop->PstrScalarOpName()->Wsz())
										);

	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, pscop, pdrgpexprArgs);

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarIsDistinctFrom
//
//	@doc:
// 		Create a scalar distinct expr from a DXL scalar distinct compare
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarIsDistinctFrom
	(
	const CDXLNode *pdxlnDistCmp
	)
{
	GPOS_ASSERT(NULL != pdxlnDistCmp && 2 == pdxlnDistCmp->UlArity());
	CDXLScalarDistinctComp *pdxlopDistCmp = CDXLScalarDistinctComp::PdxlopConvert(pdxlnDistCmp->Pdxlop());
	// get children
	CDXLNode *pdxlnLeft = (*pdxlnDistCmp)[0];
	CDXLNode *pdxlnRight = (*pdxlnDistCmp)[1];

	// translate left and right children
	CExpression *pexprLeft = Pexpr(pdxlnLeft);
	CExpression *pexprRight = Pexpr(pdxlnRight);
	
	IMDId *pmdidOp = pdxlopDistCmp->Pmdid();
	pmdidOp->AddRef();
	const IMDScalarOp *pmdscop = m_pmda->Pmdscop(pmdidOp);

	CScalarIsDistinctFrom *popScIDF = GPOS_NEW(m_pmp) CScalarIsDistinctFrom
													(
													m_pmp,
													pmdidOp,
													GPOS_NEW(m_pmp) CWStringConst(m_pmp, (pmdscop->Mdname().Pstr())->Wsz())
													);

	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, popScIDF, pexprLeft, pexprRight);

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarNullIf
//
//	@doc:
// 		Create a scalar nullif expr from a DXL scalar nullif
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarNullIf
	(
	const CDXLNode *pdxlnNullIf
	)
{
	GPOS_ASSERT(NULL != pdxlnNullIf && 2 == pdxlnNullIf->UlArity());
	CDXLScalarNullIf *pdxlop = CDXLScalarNullIf::PdxlopConvert(pdxlnNullIf->Pdxlop());

	// translate children
	CExpression *pexprLeft = Pexpr((*pdxlnNullIf)[0]);
	CExpression *pexprRight = Pexpr((*pdxlnNullIf)[1]);

	IMDId *pmdidOp = pdxlop->PmdidOp();
	pmdidOp->AddRef();

	IMDId *pmdidType = pdxlop->PmdidType();
	pmdidType->AddRef();

	return GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarNullIf(m_pmp, pmdidOp, pmdidType), pexprLeft, pexprRight);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarCmp
//
//	@doc:
// 		Create a scalar compare expr from a DXL scalar compare
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarCmp
	(
	const CDXLNode *pdxlnCmp
	)
{
	GPOS_ASSERT(NULL != pdxlnCmp && 2 == pdxlnCmp->UlArity());
	CDXLScalarComp *pdxlopComp = CDXLScalarComp::PdxlopConvert(pdxlnCmp->Pdxlop());
	// get children
	CDXLNode *pdxlnLeft = (*pdxlnCmp)[0];
	CDXLNode *pdxlnRight = (*pdxlnCmp)[1];

	// translate left and right children
	CExpression *pexprLeft = Pexpr(pdxlnLeft);
	CExpression *pexprRight = Pexpr(pdxlnRight);

	IMDId *pmdid = pdxlopComp->Pmdid();
	pmdid->AddRef();

	CScalarCmp *popScCmp = GPOS_NEW(m_pmp) CScalarCmp
										(
										m_pmp,
										pmdid,
										GPOS_NEW(m_pmp) CWStringConst(m_pmp, pdxlopComp->PstrCmpOpName()->Wsz()),
										CUtils::Ecmpt(pmdid)
										);

	GPOS_ASSERT(NULL != popScCmp);
	GPOS_ASSERT(NULL != popScCmp->Pstr());
	GPOS_ASSERT(NULL != popScCmp->Pstr()->Wsz());
	
	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, popScCmp, pexprLeft, pexprRight);

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarFunc
//
//	@doc:
// 		Create a scalar func operator expression from a DXL func expr
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarFunc
	(
	const CDXLNode *pdxlnFunc
	)
{
	GPOS_ASSERT(NULL != pdxlnFunc);

	const ULONG ulLen = pdxlnFunc->UlArity();

	CDXLScalarFuncExpr *pdxlopFuncExpr = CDXLScalarFuncExpr::PdxlopConvert(pdxlnFunc->Pdxlop());
	
	COperator *pop = NULL;

	IMDId *pmdidFunc = pdxlopFuncExpr->PmdidFunc();
	pmdidFunc->AddRef();
	const IMDFunction *pmdfunc = m_pmda->Pmdfunc(pmdidFunc);

	IMDId *pmdidRetType = pdxlopFuncExpr->PmdidRetType();
	pmdidRetType->AddRef();

	DrgPexpr *pdrgpexprArgs = NULL;
	IMDId *pmdidInput = NULL;
	if (0 < ulLen)
	{
		// translate function arguments
		pdrgpexprArgs = PdrgpexprChildren(pdxlnFunc);

		if (1 == ulLen)
		{
			CExpression *pexprFirstChild = (*pdrgpexprArgs)[0];
			COperator *popFirstChild = pexprFirstChild->Pop();
			if (popFirstChild->FScalar())
			{
				pmdidInput = CScalar::PopConvert(popFirstChild)->PmdidType();
			}
		}
	}

	if (CTranslatorDXLToExprUtils::FCastFunc(m_pmda, pdxlnFunc, pmdidInput))
	{
		const IMDCast *pmdcast = m_pmda->Pmdcast(pmdidInput, pmdidRetType);
		pop = GPOS_NEW(m_pmp) CScalarCast
				(
				m_pmp,
				pmdidRetType,
				pmdidFunc,
				pmdcast->FBinaryCoercible()
				);
	}
	else
	{
		pop = GPOS_NEW(m_pmp) CScalarFunc
				(
				m_pmp,
				pmdidFunc,
				pmdidRetType,
				GPOS_NEW(m_pmp) CWStringConst(m_pmp, (pmdfunc->Mdname().Pstr())->Wsz())
				);
	}
	
	CExpression *pexprFunc = NULL;
	if (NULL != pdrgpexprArgs)
	{
		pexprFunc = GPOS_NEW(m_pmp) CExpression(m_pmp, pop, pdrgpexprArgs);
	}
	else
	{
		pexprFunc = GPOS_NEW(m_pmp) CExpression(m_pmp, pop);
	}
	
	return pexprFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::FUnsupportedWindowFunc
//
//	@doc:
// 		Check unsupported window functions
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToExpr::FUnsupportedWindowFunc
	(
	const IMDId *pmdidFunc
	)
{
	const CMDIdGPDB *pmdidgpdb = CMDIdGPDB::PmdidConvert(pmdidFunc);
	OID oid = pmdidgpdb->OidObjectId();

	if (GPDB_PERCENT_RANK_OID == oid ||
		GPDB_CUME_DIST_OID == oid ||
		GPDB_NTILE_INT4_OID == oid ||
		GPDB_NTILE_INT8_OID == oid ||
		GPDB_NTILE_NUMERIC_OID == oid)
	{
		return true;
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprWindowFunc
//
//	@doc:
// 		Create a scalar window function expression from a DXL window ref
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprWindowFunc
	(
	const CDXLNode *pdxlnWindowRef
	)
{
	CDXLScalarWindowRef *pdxlopWinref = CDXLScalarWindowRef::PdxlopConvert(pdxlnWindowRef->Pdxlop());

	IMDId *pmdidFunc = pdxlopWinref->PmdidFunc();
	pmdidFunc->AddRef();

	CWStringConst *pstrName = GPOS_NEW(m_pmp) CWStringConst(m_pmp, CMDAccessorUtils::PstrWindowFuncName(m_pmda, pmdidFunc)->Wsz());
	if (FUnsupportedWindowFunc(pmdidFunc))
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, pstrName->Wsz());
	}

	CScalarWindowFunc::EWinStage ews = Ews(pdxlopWinref->Edxlwinstage());

	IMDId *pmdidRetType = pdxlopWinref->PmdidRetType();
	pmdidRetType->AddRef();
	
	GPOS_ASSERT(NULL != pstrName);
	CScalarWindowFunc *popWindowFunc = GPOS_NEW(m_pmp) CScalarWindowFunc(m_pmp, pmdidFunc, pmdidRetType, pstrName, ews, pdxlopWinref->FDistinct());

	CExpression *pexprWindowFunc = NULL;
	if (0 < pdxlnWindowRef->UlArity())
	{
		DrgPexpr *pdrgpexprArgs = PdrgpexprChildren(pdxlnWindowRef);

		pexprWindowFunc= GPOS_NEW(m_pmp) CExpression(m_pmp, popWindowFunc, pdrgpexprArgs);
	}
	else
	{
		// window function has no arguments
		pexprWindowFunc = GPOS_NEW(m_pmp) CExpression(m_pmp, popWindowFunc);
	}

	return pexprWindowFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Ews
//
//	@doc:
//		Translate the DXL representation of the window stage
//
//---------------------------------------------------------------------------
CScalarWindowFunc::EWinStage
CTranslatorDXLToExpr::Ews
	(
	EdxlWinStage edxlws
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
	GPOS_ASSERT(ulArity > (ULONG) edxlws  && "Invalid window stage");
#endif
	CScalarWindowFunc::EWinStage ews= (CScalarWindowFunc::EWinStage)rgrgulMapping[(ULONG) edxlws][1];
	
	return ews;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarCoalesce
//
//	@doc:
// 		Create a scalar coalesce expression from a DXL scalar coalesce
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarCoalesce
	(
	const CDXLNode *pdxlnCoalesce
	)
{
	GPOS_ASSERT(NULL != pdxlnCoalesce);
	GPOS_ASSERT(0 < pdxlnCoalesce->UlArity());

	CDXLScalarCoalesce *pdxlop = CDXLScalarCoalesce::PdxlopConvert(pdxlnCoalesce->Pdxlop());

	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxlnCoalesce);

	IMDId *pmdid = pdxlop->PmdidType();
	pmdid->AddRef();

	return GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarCoalesce(m_pmp, pmdid), pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarMinMax
//
//	@doc:
// 		Create a scalar MinMax expression from a DXL scalar MinMax
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarMinMax
	(
	const CDXLNode *pdxlnMinMax
	)
{
	GPOS_ASSERT(NULL != pdxlnMinMax);
	GPOS_ASSERT(0 < pdxlnMinMax->UlArity());

	CDXLScalarMinMax *pdxlop = CDXLScalarMinMax::PdxlopConvert(pdxlnMinMax->Pdxlop());

	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxlnMinMax);

	CDXLScalarMinMax::EdxlMinMaxType emmt = pdxlop->Emmt();
	GPOS_ASSERT(CDXLScalarMinMax::EmmtMin == emmt || CDXLScalarMinMax::EmmtMax == emmt);

	CScalarMinMax::EScalarMinMaxType esmmt = CScalarMinMax::EsmmtMin;
	if (CDXLScalarMinMax::EmmtMax == emmt)
	{
		esmmt = CScalarMinMax::EsmmtMax;
	}

	IMDId *pmdid = pdxlop->PmdidType();
	pmdid->AddRef();

	return GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarMinMax(m_pmp, pmdid, esmmt), pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprAggFunc
//
//	@doc:
// 		Create a scalar agg func operator expression from a DXL aggref node
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprAggFunc
	(
	const CDXLNode *pdxlnAggref
	)
{
	CDXLScalarAggref *pdxlop = CDXLScalarAggref::PdxlopConvert(pdxlnAggref->Pdxlop());
	
	IMDId *pmdidAggFunc = pdxlop->PmdidAgg();
	pmdidAggFunc->AddRef();
	const IMDAggregate *pmdagg = m_pmda->Pmdagg(pmdidAggFunc);
	
	EAggfuncStage eaggfuncstage = EaggfuncstageLocal;
	if (EdxlaggstagePartial != pdxlop->Edxlaggstage())
	{
		eaggfuncstage = EaggfuncstageGlobal;
	}
	BOOL fSplit = (EdxlaggstageNormal != pdxlop->Edxlaggstage());

	IMDId *pmdidResolvedReturnType = pdxlop->PmdidResolvedRetType();
	if (NULL != pmdidResolvedReturnType)
	{
		// use the resolved type provided in DXL
		pmdidResolvedReturnType->AddRef();
	}

	CScalarAggFunc *popScAggFunc =
			CUtils::PopAggFunc
				(
				m_pmp,
				pmdidAggFunc,
				GPOS_NEW(m_pmp) CWStringConst(m_pmp, (pmdagg->Mdname().Pstr())->Wsz()),
				pdxlop->FDistinct(),
				eaggfuncstage,
				fSplit,
				pmdidResolvedReturnType
				);

	CExpression *pexprAggFunc = NULL;
	
	if (0 < pdxlnAggref->UlArity())
	{
		// translate function arguments
		DrgPexpr *pdrgpexprArgs = PdrgpexprChildren(pdxlnAggref);

		pexprAggFunc= GPOS_NEW(m_pmp) CExpression(m_pmp, popScAggFunc, pdrgpexprArgs);
	}
	else
	{
		// aggregate function has no arguments
		pexprAggFunc = GPOS_NEW(m_pmp) CExpression(m_pmp, popScAggFunc);
	}
	
	return pexprAggFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprArray
//
//	@doc:
// 		Translate a scalar array
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprArray
	(
	const CDXLNode *pdxln
	)
{
	CDXLScalarArray *pdxlop = CDXLScalarArray::PdxlopConvert(pdxln->Pdxlop());
	
	IMDId *pmdidElem = pdxlop->PmdidElem();
	pmdidElem->AddRef();

	IMDId *pmdidArray = pdxlop->PmdidArray();
	pmdidArray->AddRef();

	CScalarArray *popArray = GPOS_NEW(m_pmp) CScalarArray(m_pmp, pmdidElem, pmdidArray, pdxlop->FMultiDimensional());
	
	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxln);

	return GPOS_NEW(m_pmp) CExpression(m_pmp, popArray, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprArrayRef
//
//	@doc:
// 		Translate a scalar arrayref
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprArrayRef
	(
	const CDXLNode *pdxln
	)
{
	CDXLScalarArrayRef *pdxlop = CDXLScalarArrayRef::PdxlopConvert(pdxln->Pdxlop());

	IMDId *pmdidElem = pdxlop->PmdidElem();
	pmdidElem->AddRef();

	IMDId *pmdidArray = pdxlop->PmdidArray();
	pmdidArray->AddRef();

	IMDId *pmdidReturn = pdxlop->PmdidReturn();
	pmdidReturn->AddRef();

	CScalarArrayRef *popArrayref = GPOS_NEW(m_pmp) CScalarArrayRef(m_pmp, pmdidElem, pmdidArray, pmdidReturn);

	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxln);

	return GPOS_NEW(m_pmp) CExpression(m_pmp, popArrayref, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprArrayRefIndexList
//
//	@doc:
// 		Translate a scalar arrayref index list
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprArrayRefIndexList
	(
	const CDXLNode *pdxln
	)
{
	CDXLScalarArrayRefIndexList *pdxlop = CDXLScalarArrayRefIndexList::PdxlopConvert(pdxln->Pdxlop());
	CScalarArrayRefIndexList *popIndexlist = GPOS_NEW(m_pmp) CScalarArrayRefIndexList(m_pmp, Eilt(pdxlop->Eilb()));

	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxln);

	return GPOS_NEW(m_pmp) CExpression(m_pmp, popIndexlist, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Eilt
//
//	@doc:
// 		Translate the arrayref index list type
//
//---------------------------------------------------------------------------
CScalarArrayRefIndexList::EIndexListType
CTranslatorDXLToExpr::Eilt
	(
	const CDXLScalarArrayRefIndexList::EIndexListBound eilb
	)
{
	switch (eilb)
	{
		case CDXLScalarArrayRefIndexList::EilbLower:
			return CScalarArrayRefIndexList::EiltLower;

		case CDXLScalarArrayRefIndexList::EilbUpper:
			return CScalarArrayRefIndexList::EiltUpper;

		default:
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, GPOS_WSZ_LIT("Invalid arrayref index type"));
			return CScalarArrayRefIndexList::EiltSentinel;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprArrayCmp
//
//	@doc:
// 		Translate a scalar array compare
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprArrayCmp
	(
	const CDXLNode *pdxln
	)
{
	CDXLScalarArrayComp *pdxlop = CDXLScalarArrayComp::PdxlopConvert(pdxln->Pdxlop());
	
	IMDId *pmdidOp = pdxlop->Pmdid();
	pmdidOp->AddRef();

	const CWStringConst *pstrOpName = pdxlop->PstrCmpOpName();
	
	EdxlArrayCompType edxlarrcmp = pdxlop->Edxlarraycomptype();
	CScalarArrayCmp::EArrCmpType earrcmpt = CScalarArrayCmp::EarrcmpSentinel;
	if (Edxlarraycomptypeall == edxlarrcmp)
	{
		earrcmpt = CScalarArrayCmp::EarrcmpAll;
	}
	else
	{
		GPOS_ASSERT(Edxlarraycomptypeany == edxlarrcmp);
		earrcmpt = CScalarArrayCmp::EarrcmpAny;
	}
	
	CScalarArrayCmp *popArrayCmp = GPOS_NEW(m_pmp) CScalarArrayCmp(m_pmp, pmdidOp, GPOS_NEW(m_pmp) CWStringConst(m_pmp, pstrOpName->Wsz()), earrcmpt);
	
	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxln);

	return GPOS_NEW(m_pmp) CExpression(m_pmp, popArrayCmp, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarIdent
//
//	@doc:
// 		Create a scalar ident expr from a DXL scalar ident
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarIdent
	(
	const CDXLNode *pdxlnIdent
	)
{
	// get dxl scalar identifier
	CDXLScalarIdent *pdxlop = CDXLScalarIdent::PdxlopConvert(pdxlnIdent->Pdxlop());

	// get the dxl column reference
	const CDXLColRef *pdxlcr = pdxlop->Pdxlcr();
	const ULONG ulColId = pdxlcr->UlID();

	// get its column reference from the hash map
	const CColRef *pcr =  PcrLookup(m_phmulcr, ulColId);
	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarIdent(m_pmp, pcr));

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PdrgpexprChildren
//
//	@doc:
// 		Translate children of a DXL node
//
//---------------------------------------------------------------------------
DrgPexpr *
CTranslatorDXLToExpr::PdrgpexprChildren
	(
	const CDXLNode *pdxln
	)
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);

	const ULONG ulArity = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		// get next child and translate it
		CDXLNode *pdxlnChild = (*pdxln)[ul];

		CExpression *pexprChild = Pexpr(pdxlnChild);
		pdrgpexpr->Append(pexprChild);
	}

	return pdrgpexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarIf
//
//	@doc:
// 		Create a scalar if expression from a DXL scalar if statement
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarIf
	(
	const CDXLNode *pdxlnIfStmt
	)
{
	GPOS_ASSERT(NULL != pdxlnIfStmt);
	GPOS_ASSERT(3 == pdxlnIfStmt->UlArity());

	CDXLScalarIfStmt *pdxlop = CDXLScalarIfStmt::PdxlopConvert(pdxlnIfStmt->Pdxlop());

	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxlnIfStmt);

	IMDId *pmdid = pdxlop->PmdidResultType();
	pmdid->AddRef();
	CScalarIf *popScIf = GPOS_NEW(m_pmp) CScalarIf(m_pmp, pmdid);
	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, popScIf, pdrgpexprChildren);

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarSwitch
//
//	@doc:
// 		Create a scalar switch expression from a DXL scalar switch
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarSwitch
	(
	const CDXLNode *pdxlnSwitch
	)
{
	GPOS_ASSERT(NULL != pdxlnSwitch);
	GPOS_ASSERT(1 < pdxlnSwitch->UlArity());

	CDXLScalarSwitch *pdxlop = CDXLScalarSwitch::PdxlopConvert(pdxlnSwitch->Pdxlop());

	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxlnSwitch);

	IMDId *pmdid = pdxlop->PmdidType();
	pmdid->AddRef();
	CScalarSwitch *pop = GPOS_NEW(m_pmp) CScalarSwitch(m_pmp, pmdid);

	return GPOS_NEW(m_pmp) CExpression(m_pmp, pop, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarSwitchCase
//
//	@doc:
// 		Create a scalar switchcase expression from a DXL scalar switchcase
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarSwitchCase
	(
	const CDXLNode *pdxlnSwitchCase
	)
{
	GPOS_ASSERT(NULL != pdxlnSwitchCase);

	GPOS_ASSERT(2 == pdxlnSwitchCase->UlArity());

	DrgPexpr *pdrgpexprChildren = PdrgpexprChildren(pdxlnSwitchCase);

	return GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarSwitchCase(m_pmp), pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarCaseTest
//
//	@doc:
// 		Create a scalar case test expression from a DXL scalar case test
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarCaseTest
	(
	const CDXLNode *pdxlnCaseTest
	)
{
	GPOS_ASSERT(NULL != pdxlnCaseTest);

	CDXLScalarCaseTest *pdxlop =
			CDXLScalarCaseTest::PdxlopConvert(pdxlnCaseTest->Pdxlop());

	IMDId *pmdid = pdxlop->PmdidType();
	pmdid->AddRef();
	CScalarCaseTest *pop = GPOS_NEW(m_pmp) CScalarCaseTest(m_pmp, pmdid);

	return GPOS_NEW(m_pmp) CExpression(m_pmp, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarNullTest
//
//	@doc:
// 		Create a scalar null test expr from a DXL scalar null test
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarNullTest
	(
	const CDXLNode *pdxlnNullTest
	)
{
	// get dxl scalar null test
	CDXLScalarNullTest *pdxlop =
			CDXLScalarNullTest::PdxlopConvert(pdxlnNullTest->Pdxlop());

	GPOS_ASSERT(NULL != pdxlop);

	// translate child expression
	GPOS_ASSERT(1 == pdxlnNullTest->UlArity());
	
	CDXLNode *pdxlnChild = (*pdxlnNullTest)[0];
	CExpression *pexprChild = Pexpr(pdxlnChild);
	
	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarNullTest(m_pmp), pexprChild);
	
	if (!pdxlop->FIsNullTest())
	{
		// IS NOT NULL test: add a not expression on top
		pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarBoolOp(m_pmp, CScalarBoolOp::EboolopNot), pexpr);
	}

	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarBooleanTest
//
//	@doc:
// 		Create a scalar boolean test expr from a DXL scalar boolean test
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarBooleanTest
	(
	const CDXLNode *pdxlnScBoolTest
	)
{
	const ULONG rgulBoolTestMapping[][2] =
	{
		{EdxlbooleantestIsTrue, CScalarBooleanTest::EbtIsTrue},
		{EdxlbooleantestIsNotTrue, CScalarBooleanTest::EbtIsNotTrue},
		{EdxlbooleantestIsFalse, CScalarBooleanTest::EbtIsFalse},
		{EdxlbooleantestIsNotFalse, CScalarBooleanTest::EbtIsNotFalse},
		{EdxlbooleantestIsUnknown, CScalarBooleanTest::EbtIsUnknown},
		{EdxlbooleantestIsNotUnknown, CScalarBooleanTest::EbtIsNotUnknown},
	};

	// get dxl scalar null test
	CDXLScalarBooleanTest *pdxlop =
			CDXLScalarBooleanTest::PdxlopConvert(pdxlnScBoolTest->Pdxlop());

	GPOS_ASSERT(NULL != pdxlop);

	// translate child expression
	GPOS_ASSERT(1 == pdxlnScBoolTest->UlArity());

	CDXLNode *pdxlnChild = (*pdxlnScBoolTest)[0];
	CExpression *pexprChild = Pexpr(pdxlnChild);

	CScalarBooleanTest::EBoolTest ebt = (CScalarBooleanTest::EBoolTest) (rgulBoolTestMapping[pdxlop->EdxlBoolType()][1]);
	
	return GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarBooleanTest(m_pmp, ebt), pexprChild);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarCast
//
//	@doc:
// 		Create a scalar relabel type from a DXL scalar cast
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarCast
	(
	const CDXLNode *pdxlnCast
	)
{
	// get dxl scalar relabel type
	CDXLScalarCast *pdxlop = CDXLScalarCast::PdxlopConvert(pdxlnCast->Pdxlop());
	GPOS_ASSERT(NULL != pdxlop);

	// translate child expression
	GPOS_ASSERT(1 == pdxlnCast->UlArity());
	CDXLNode *pdxlnChild = (*pdxlnCast)[0];
	CExpression *pexprChild = Pexpr(pdxlnChild);

	IMDId *pmdidType = pdxlop->PmdidType();
	IMDId *pmdidFunc = pdxlop->PmdidFunc();
	pmdidType->AddRef();
	pmdidFunc->AddRef();
	
	COperator *popChild = pexprChild->Pop();
	IMDId *pmdidInput = CScalar::PopConvert(popChild)->PmdidType();
	const IMDCast *pmdcast = m_pmda->Pmdcast(pmdidInput, pmdidType);
	BOOL fRelabel = pmdcast->FBinaryCoercible();

	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression
									(
									m_pmp,
									GPOS_NEW(m_pmp) CScalarCast(m_pmp, pmdidType, pmdidFunc, fRelabel),
									pexprChild
									);

	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarCoerceToDomain
//
//	@doc:
// 		Create a scalar CoerceToDomain from a DXL scalar CoerceToDomain
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarCoerceToDomain
	(
	const CDXLNode *pdxlnCoerce
	)
{
	// get dxl scalar coerce operator
	CDXLScalarCoerceToDomain *pdxlop = CDXLScalarCoerceToDomain::PdxlopConvert(pdxlnCoerce->Pdxlop());
	GPOS_ASSERT(NULL != pdxlop);

	// translate child expression
	GPOS_ASSERT(1 == pdxlnCoerce->UlArity());
	CDXLNode *pdxlnChild = (*pdxlnCoerce)[0];
	CExpression *pexprChild = Pexpr(pdxlnChild);

	IMDId *pmdidType = pdxlop->PmdidResultType();
	pmdidType->AddRef();

	EdxlCoercionForm edxlcf = pdxlop->Edxlcf();

	return GPOS_NEW(m_pmp) CExpression
				(
				m_pmp,
				GPOS_NEW(m_pmp) CScalarCoerceToDomain
						(
						m_pmp,
						pmdidType,
						pdxlop->IMod(),
						(ECoercionForm) edxlcf, // map Coercion Form directly based on position in enum
						pdxlop->ILoc()
						),
				pexprChild
				);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarConst
//
//	@doc:
// 		Create a scalar const expr from a DXL scalar constant value
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarConst
	(
	const CDXLNode *pdxlnConstVal
	)
{
	GPOS_ASSERT(NULL != pdxlnConstVal);

	// translate the dxl scalar const value
	CDXLScalarConstValue *pdxlop =
			CDXLScalarConstValue::PdxlopConvert(pdxlnConstVal->Pdxlop());
	CScalarConst *popConst = CTranslatorDXLToExprUtils::PopConst(m_pmp, m_pmda, pdxlop);
	
	return GPOS_NEW(m_pmp) CExpression(m_pmp, popConst);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarSubquery
//
//	@doc:
// 		Create a scalar subquery expr from a DXL scalar subquery node
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarSubquery
	(
	const CDXLNode *pdxlnSubquery
	)
{
	GPOS_ASSERT(NULL != pdxlnSubquery);
	CDXLScalarSubquery *pdxlopSubquery =
			CDXLScalarSubquery::PdxlopConvert(pdxlnSubquery->Pdxlop());

	// translate child
	CDXLNode *pdxlnChild = (*pdxlnSubquery)[0];
	CExpression *pexprChild = Pexpr(pdxlnChild);
	
	// get subquery colref for colid
	ULONG ulColId = pdxlopSubquery->UlColId();
	const CColRef *pcr = PcrLookup(m_phmulcr, ulColId);
		
	CScalarSubquery *popScalarSubquery = GPOS_NEW(m_pmp) CScalarSubquery(m_pmp, pcr, false /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/);
	GPOS_ASSERT(NULL != popScalarSubquery);
	CExpression *pexpr = GPOS_NEW(m_pmp) CExpression(m_pmp, popScalarSubquery, pexprChild);

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarProjElem
//
//	@doc:
// 		Create a scalar project elem expression from a DXL scalar project elem node
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarProjElem
	(
	const CDXLNode *pdxlnPrEl
	)
{
	GPOS_ASSERT(NULL != pdxlnPrEl && EdxlopScalarProjectElem == pdxlnPrEl->Pdxlop()->Edxlop());
	GPOS_ASSERT(1 == pdxlnPrEl->UlArity());
	
	CDXLScalarProjElem *pdxlopPrEl = CDXLScalarProjElem::PdxlopConvert(pdxlnPrEl->Pdxlop());
	
	// translate child
	CDXLNode *pdxlnChild = (*pdxlnPrEl)[0];
	CExpression *pexprChild = Pexpr(pdxlnChild);

	CScalar *popScalar = CScalar::PopConvert(pexprChild->Pop());

	IMDId *pmdid = popScalar->PmdidType();
	const IMDType *pmdtype = m_pmda->Pmdtype(pmdid);

	CName name(pdxlopPrEl->PmdnameAlias()->Pstr());
	
	// generate a new column reference
	CColRef *pcr = m_pcf->PcrCreate(pmdtype, name);
	
	// store colid -> colref mapping
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif
	m_phmulcr->FInsert(GPOS_NEW(m_pmp) ULONG(pdxlopPrEl->UlId()), pcr);
	
	GPOS_ASSERT(fInserted);
	
	CExpression *pexprProjElem = GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarProjectElement(m_pmp, pcr), pexprChild);
	return pexprProjElem;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::PexprScalarProjList
//
//	@doc:
// 		Create a scalar project list expression from a DXL scalar project list node
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExpr::PexprScalarProjList
	(
	const CDXLNode *pdxlnPrL
	)
{
	GPOS_ASSERT(NULL != pdxlnPrL &&  EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());
	
	// translate project elements
	CExpression *pexprProjList = NULL;
	
	if (0 == pdxlnPrL->UlArity())
	{
		pexprProjList = GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarProjectList(m_pmp));
	}
	else
	{	
		DrgPexpr *pdrgpexprProjElems = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);

		const ULONG ulLen = pdxlnPrL->UlArity();
		for (ULONG ul = 0; ul < ulLen; ul++)
		{
			CDXLNode *pdxlnProjElem = (*pdxlnPrL)[ul];
			CExpression *pexprProjElem = PexprScalarProjElem(pdxlnProjElem);
			pdrgpexprProjElems->Append(pexprProjElem);
		}
		
		pexprProjList = GPOS_NEW(m_pmp) CExpression(m_pmp, GPOS_NEW(m_pmp) CScalarProjectList(m_pmp), pdrgpexprProjElems);
	}
	
	return pexprProjList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::Pos
//
//	@doc:
// 		Construct an order spec from a DXL sort col list node
//
//---------------------------------------------------------------------------
COrderSpec *
CTranslatorDXLToExpr::Pos
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);
	
	COrderSpec *pos = GPOS_NEW(m_pmp) COrderSpec(m_pmp);
	
	const ULONG ulLen = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CDXLNode *pdxlnSortCol = (*pdxln)[ul];
		
		CDXLScalarSortCol *pdxlop = CDXLScalarSortCol::PdxlopConvert(pdxlnSortCol->Pdxlop());
		const ULONG ulColId = pdxlop->UlColId();

		// get its column reference from the hash map
		CColRef *pcr =  PcrLookup(m_phmulcr, ulColId);
		
		IMDId *pmdidSortOp = pdxlop->PmdidSortOp();
		pmdidSortOp->AddRef();
		
		COrderSpec::ENullTreatment ent = COrderSpec::EntLast;
		if (pdxlop->FSortNullsFirst())
		{
			ent = COrderSpec::EntFirst;
		}
		
		pos->Append(pmdidSortOp, pcr, ent);
	}
	
	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExpr::AddDistributionColumns
//
//	@doc:
// 		Add distribution column info from the MD relation to the table descriptor
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToExpr::AddDistributionColumns
	(
	CTableDescriptor *ptabdesc,
	const IMDRelation *pmdrel,
	HMIUl *phmiulAttnoColMapping
	)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pmdrel);
	
	// compute distribution columns for table descriptor
	ULONG ulCols = pmdrel->UlDistrColumns();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		const IMDColumn *pmdcol = pmdrel->PmdcolDistrColumn(ul);
		INT iAttno = pmdcol->IAttno();
		ULONG *pulPos = phmiulAttnoColMapping->PtLookup(&iAttno);
		GPOS_ASSERT(NULL != pulPos);
		
		ptabdesc->AddDistributionColumn(*pulPos);
	}
}

// EOF
