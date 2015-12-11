//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTranslatorDXLToExprUtils.cpp
//
//	@doc:
//		Implementation of the helper methods used during DXL to Expr translation
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/translate/CTranslatorDXLToExprUtils.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "naucrates/base/IDatumInt4.h"
#include "naucrates/base/IDatumInt8.h"

#include "naucrates/dxl/operators/CDXLScalarFuncExpr.h"
#include "naucrates/dxl/operators/CDXLScalarIdent.h"

#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/IMDCast.h"

using namespace gpos;
using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::PopConst
//
//	@doc:
// 		Construct const operator from a DXL const value operator
//
//---------------------------------------------------------------------------
CScalarConst *
CTranslatorDXLToExprUtils::PopConst
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const CDXLScalarConstValue *pdxlop
	)
{
	IDatum *pdatum = CTranslatorDXLToExprUtils::Pdatum(pmda, pdxlop);
	return GPOS_NEW(pmp) CScalarConst(pmp, pdatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::Pdatum
//
//	@doc:
// 		Construct a datum from a DXL const value operator
//
//---------------------------------------------------------------------------
IDatum *
CTranslatorDXLToExprUtils::Pdatum
	(
	CMDAccessor *pmda,
	const CDXLScalarConstValue *pdxlop
	)
{
	IMDId *pmdid = pdxlop->Pdxldatum()->Pmdid();
	IDatum *pdatum = pmda->Pmdtype(pmdid)->Pdatum(pdxlop);

	return pdatum;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::Pdrgpdatum
//
//	@doc:
// 		Construct a datum array from a DXL datum array
//
//---------------------------------------------------------------------------
DrgPdatum *
CTranslatorDXLToExprUtils::Pdrgpdatum
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const DrgPdxldatum *pdrgpdxldatum
	)
{
	GPOS_ASSERT(NULL != pdrgpdxldatum);

	DrgPdatum *pdrgdatum = GPOS_NEW(pmp) DrgPdatum(pmp);
	const ULONG ulLen = pdrgpdxldatum->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CDXLDatum *pdxldatum = (*pdrgpdxldatum)[ul];
		IMDId *pmdid = pdxldatum->Pmdid();
		IDatum *pdatum = pmda->Pmdtype(pmdid)->Pdatum(pmp, pdxldatum);
		pdrgdatum->Append(pdatum);
	}

	return pdrgdatum;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::PexprConstInt8
//
//	@doc:
// 		Construct an expression representing the given value in INT8 format
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExprUtils::PexprConstInt8
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CSystemId sysid,
	LINT lVal
	)
{
	IDatumInt8 *pdatum = pmda->PtMDType<IMDTypeInt8>(sysid)->PdatumInt8(pmp, lVal, false /* fNull */);
	CExpression *pexprConst = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarConst(pmp, pdatum));

	return pexprConst;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::AddKeySets
//
//	@doc:
// 		Add key sets info from the MD relation to the table descriptor
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToExprUtils::AddKeySets
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	const IMDRelation *pmdrel,
	HMUlUl *phmululColMapping
	)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pmdrel);

	const ULONG ulKeySets = pmdrel->UlKeySets();
	for (ULONG ul = 0; ul < ulKeySets; ul++)
	{
		CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, ptabdesc->UlColumns());
		const DrgPul *pdrgpulKeys = pmdrel->PdrgpulKeyset(ul);
		const ULONG ulKeys = pdrgpulKeys->UlLength();

		for (ULONG ulKey = 0; ulKey < ulKeys; ulKey++)
		{
			// populate current keyset
			ULONG ulOriginalKey = *((*pdrgpulKeys)[ulKey]);
			ULONG *pulRemappedKey = phmululColMapping->PtLookup(&ulOriginalKey);
			GPOS_ASSERT(NULL != pulRemappedKey);
			
			pbs->FExchangeSet(*pulRemappedKey);
		}

		if (!ptabdesc->FAddKeySet(pbs))
		{
			pbs->Release();
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::FScalarBool
//
//	@doc:
//		Check if a dxl node is a scalar bool
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToExprUtils::FScalarBool
	(
	const CDXLNode *pdxln,
	EdxlBoolExprType edxlboolexprtype
	)
{
	GPOS_ASSERT(NULL != pdxln);

	CDXLOperator *pdxlop = pdxln->Pdxlop();
	if (EdxlopScalarBoolExpr == pdxlop->Edxlop())
	{
		EdxlBoolExprType edxlboolexprtypeNode =
				CDXLScalarBoolExpr::PdxlopConvert(pdxlop)->EdxlBoolType();
		return edxlboolexprtype == edxlboolexprtypeNode;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::EBoolOperator
//
//	@doc:
// 		returns the equivalent bool expression type in the optimizer for
// 		a given DXL bool expression type

//---------------------------------------------------------------------------
CScalarBoolOp::EBoolOperator
CTranslatorDXLToExprUtils::EBoolOperator
	(
	EdxlBoolExprType edxlbooltype
	)
{
	CScalarBoolOp::EBoolOperator eboolop = CScalarBoolOp::EboolopSentinel;

	switch (edxlbooltype)
	{
		case Edxlnot:
			eboolop = CScalarBoolOp::EboolopNot;
			break;
		case Edxland:
			eboolop = CScalarBoolOp::EboolopAnd;
			break;
		case Edxlor:
			eboolop = CScalarBoolOp::EboolopOr;
			break;
		default:
			GPOS_ASSERT(!"Unrecognized boolean expression type");
	}

	GPOS_ASSERT(CScalarBoolOp::EboolopSentinel > eboolop);

	return eboolop;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::Pdrgpcr
//
//	@doc:
// 		Construct a dynamic array of column references corresponding to the
//		given col ids
//
//---------------------------------------------------------------------------
DrgPcr *
CTranslatorDXLToExprUtils::Pdrgpcr
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	const DrgPul *pdrgpulColIds
	)
{
	GPOS_ASSERT(NULL != pdrgpulColIds);

	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);

	for (ULONG ul = 0; ul < pdrgpulColIds->UlLength(); ul++)
	{
		ULONG *pulColId = (*pdrgpulColIds)[ul];
		const CColRef *pcr = phmulcr->PtLookup(pulColId);
		GPOS_ASSERT(NULL != pcr);

		pdrgpcr->Append(const_cast<CColRef*>(pcr));
	}

	return pdrgpcr;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprUtils::FCastFunc
//
//	@doc:
//		Is the given expression is a scalar function that is used to cast
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToExprUtils::FCastFunc
	(
	CMDAccessor *pmda,
	const CDXLNode *pdxln,
	IMDId *pmdidInput
	)
{
	GPOS_ASSERT(NULL != pdxln);

	if (1 != pdxln->UlArity())
	{
		return false;
	}

	if (NULL == pmdidInput)
	{
		return false;
	}

	if (EdxlopScalarFuncExpr != pdxln->Pdxlop()->Edxlop())
	{
		return false;
	}

	CDXLScalarFuncExpr *pdxlopScFunc = CDXLScalarFuncExpr::PdxlopConvert(pdxln->Pdxlop());

	IMDId *pmdidDest = pdxlopScFunc->PmdidRetType();

	if(!CMDAccessorUtils::FCastExists(pmda, pmdidInput, pmdidDest))
	{
		return false;
	}

	const IMDCast *pmdcast = pmda->Pmdcast(pmdidInput, pmdidDest);

	return (pmdcast->PmdidCastFunc()->FEquals(pdxlopScFunc->PmdidFunc()));
}


// EOF
