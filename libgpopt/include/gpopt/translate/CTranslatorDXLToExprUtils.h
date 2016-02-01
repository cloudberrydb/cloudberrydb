//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTranslatorDXLToExprUtils.h
//
//	@doc:
//		Class providing helper methods for translating from DXL to Expr
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CTranslatorDXLToExprUtils_H
#define GPOPT_CTranslatorDXLToExprUtils_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalarBoolExpr.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/ops.h"

namespace gpmd
{
	class IMDRelation;
}

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpdxl;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorDXLToExprUtils
	//
	//	@doc:
	//		Class providing helper methods for translating from DXL to Expr
	//
	//---------------------------------------------------------------------------
	class CTranslatorDXLToExprUtils
	{
		public:

			// create a cast expression from INT to INT8
			static
			CExpression *PexprConstInt8
							(
							IMemoryPool *pmp,
							CMDAccessor *pmda,
							CSystemId sysid,
							LINT liValue
							);

			// create a scalar const operator from a DXL scalar const operator
			static
			CScalarConst *PopConst
							(
							IMemoryPool *pmp,
							CMDAccessor *pmda,
							const CDXLScalarConstValue *pdxlop
							);

			// create a datum from a DXL scalar const operator
			static
			IDatum *Pdatum(CMDAccessor *pmda, const CDXLScalarConstValue *pdxlop);

			// create a datum array from a dxl datum array
			static
			DrgPdatum *Pdrgpdatum(IMemoryPool *pmp, CMDAccessor *pmda, const DrgPdxldatum *pdrgpdatum);

			// update table descriptor's key sets info from the MD cache object
			static
			void AddKeySets
					(
					IMemoryPool *pmp,
					CTableDescriptor *ptabdesc,
					const IMDRelation *pmdrel,
					HMUlUl *phmululColMapping
					);

			// check if a dxl node is a boolean expression of the given type
			static
			BOOL FScalarBool(const CDXLNode *pdxln, EdxlBoolExprType edxlboolexprtype);

			// returns the equivalent bool expression type in the optimizer for
			// a given DXL bool expression type
			static
			CScalarBoolOp::EBoolOperator EBoolOperator(EdxlBoolExprType edxlbooltype);

			// construct a dynamic array of col refs corresponding to the given col ids
			static
			DrgPcr *Pdrgpcr(IMemoryPool *pmp, HMUlCr *phmulcr, const DrgPul *pdrgpulColIds);

			// is the given expression is a scalar function that casts
			static
			BOOL FCastFunc(CMDAccessor *pmda, const CDXLNode *pdxln, IMDId *pmdidInput);
	};
}

#endif // !GPOPT_CTranslatorDXLToExprUtils_H

// EOF
