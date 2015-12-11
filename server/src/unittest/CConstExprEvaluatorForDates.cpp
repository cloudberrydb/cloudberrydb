//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CConstExprEvaluatorForDates.cpp
//
//	@doc:
//		Implementation of a constant expression evaluator for dates data
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/base/IDatumStatisticsMappable.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDType.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"

#include "unittest/gpopt/CConstExprEvaluatorForDates.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorForDates::PexprEval
//
//	@doc:
//		It expects that the given expression is a scalar comparison between
//		two date constants. It compares the two constants using their double
//		stats mapping, which in the case of the date type gives a correct result.
//		If it gets an illegal expression, an assertion failure is raised in
//		debug mode.
//
//---------------------------------------------------------------------------
CExpression *
CConstExprEvaluatorForDates::PexprEval
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(COperator::EopScalarCmp == pexpr->Pop()->Eopid());
	GPOS_ASSERT(COperator::EopScalarConst == (*pexpr)[0]->Pop()->Eopid());
	GPOS_ASSERT(COperator::EopScalarConst == (*pexpr)[1]->Pop()->Eopid());

	CScalarConst *popScalarLeft = dynamic_cast<CScalarConst *>((*pexpr)[0]->Pop());

	GPOS_ASSERT(CMDIdGPDB::m_mdidDate.FEquals(popScalarLeft->Pdatum()->Pmdid()));
	IDatumStatisticsMappable *pdatumLeft = dynamic_cast<IDatumStatisticsMappable *>(popScalarLeft->Pdatum());
	CScalarConst *popScalarRight = dynamic_cast<CScalarConst *>((*pexpr)[1]->Pop());

	GPOS_ASSERT(CMDIdGPDB::m_mdidDate.FEquals(popScalarRight->Pdatum()->Pmdid()));
	IDatumStatisticsMappable *pdatumRight = dynamic_cast<IDatumStatisticsMappable *>(popScalarRight->Pdatum());

	CScalarCmp *popScCmp = dynamic_cast<CScalarCmp *>(pexpr->Pop());
	CDouble dLeft = pdatumLeft->DStatsMapping();
	CDouble dRight = pdatumRight->DStatsMapping();
	BOOL fResult = false;
	switch (popScCmp->Ecmpt())
	{
		case IMDType::EcmptEq:
			fResult = dLeft == dRight;
			break;
		case IMDType::EcmptNEq:
			fResult = dLeft != dRight;
			break;
		case IMDType::EcmptL:
			fResult = dLeft < dRight;
			break;
		case IMDType::EcmptLEq:
			fResult = dLeft <= dRight;
			break;
		case IMDType::EcmptG:
			fResult = dLeft > dRight;
			break;
		case IMDType::EcmptGEq:
			fResult = dLeft >= dRight;
			break;
		default:
			GPOS_ASSERT(false && "Unsupported comparison");
			return NULL;
	}
	CExpression *pexprResult = CUtils::PexprScalarConstBool(m_pmp, fResult);

	return pexprResult;
}

// EOF
