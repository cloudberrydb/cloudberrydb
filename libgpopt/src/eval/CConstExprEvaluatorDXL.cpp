//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CConstExprEvaluatorDXL.cpp
//
//	@doc:
//		Constant expression evaluator implementation that delegats to a DXL evaluator
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/eval/CConstExprEvaluatorDXL.h"
#include "gpopt/eval/IConstDXLNodeEvaluator.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/CExpression.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;
using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXL::CConstExprEvaluatorDXL
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstExprEvaluatorDXL::CConstExprEvaluatorDXL
	(
	IMemoryPool *mp,
	CMDAccessor *md_accessor,
	IConstDXLNodeEvaluator *pconstdxleval
	)
	:
	m_pconstdxleval(pconstdxleval),
	m_trexpr2dxl(mp, md_accessor, NULL /*pdrgpiSegments*/, false /*fInitColumnFactory*/),
	m_trdxl2expr(mp, md_accessor, false /*fInitColumnFactory*/)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXL::~CConstExprEvaluatorDXL
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstExprEvaluatorDXL::~CConstExprEvaluatorDXL()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXL::FValidInput
//
//	@doc:
//		Checks if the given expression is a valid input for constant expression evaluation,
//		i.e., a scalar expression without variables and without subqueries.
//		If it returns false, it sets the contents of the error message appropriately.
//		If szErrorMsg is not null and this function returns false, szErrorMsg will contain
//		the error message.
//
//---------------------------------------------------------------------------
BOOL
CConstExprEvaluatorDXL::FValidInput
	(
	CExpression *pexpr,
	const CHAR **szErrorMsg
	)
{
	GPOS_ASSERT(NULL != pexpr);
	// if the expression is not scalar, we should not try to evaluate it
	if (!pexpr->Pop()->FScalar())
	{
		if (NULL != szErrorMsg)
		{
			*szErrorMsg = pexpr->Pop()->SzId();
		}
		return false;
	}
	// if the expression has a subquery, we should not try to evaluate it
	if (CDrvdPropScalar::GetDrvdScalarProps(pexpr->PdpDerive())->FHasSubquery())
	{
		if (NULL != szErrorMsg)
		{
			*szErrorMsg = "expression containing subqueries";
		}
		return false;
	}
	// if the expression uses or defines any variables, we should not try to evaluate it
	CDrvdPropScalar *pdpScalar = CDrvdPropScalar::GetDrvdScalarProps(pexpr->PdpDerive());
	if (0 != pdpScalar->PcrsUsed()->Size() ||
		0 != pdpScalar->PcrsDefined()->Size())
	{
		if (NULL != szErrorMsg)
		{
			*szErrorMsg = "expression with variables";
		}
		return false;
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXL::PexprEval
//
//	@doc:
//		Evaluate the given expression and return the result as a new expression.
//		Caller takes ownership of returned expression
//
//---------------------------------------------------------------------------
CExpression *
CConstExprEvaluatorDXL::PexprEval
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	const CHAR *szErrorMsg = "";
	if (!FValidInput(pexpr, &szErrorMsg))
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiEvalUnsupportedScalarExpr, szErrorMsg);
	}
	CDXLNode *pdxlnExpr = m_trexpr2dxl.PdxlnScalar(pexpr);
	CDXLNode *pdxlnResult = m_pconstdxleval->EvaluateExpr(pdxlnExpr);

	GPOS_ASSERT(EdxloptypeScalar == pdxlnResult->GetOperator()->GetDXLOperatorType());

	CExpression *pexprResult = m_trdxl2expr.PexprTranslateScalar(pdxlnResult, NULL /*colref_array*/);
	pdxlnResult->Release();
	pdxlnExpr->Release();

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXL::FCanEvalExpressions
//
//	@doc:
//		Returns true, since this evaluator always attempts to evaluate the expression and compute a datum
//
//---------------------------------------------------------------------------
BOOL CConstExprEvaluatorDXL::FCanEvalExpressions()
{
	return m_pconstdxleval->FCanEvalExpressions();
}



// EOF
