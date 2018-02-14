//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CScalarArrayCoerceExpr.h
//
//	@doc:
//		Scalar Array Coerce Expr operator,
//		the operator will apply type casting for each element in this array
//		using the given element coercion function.
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarArrayCoerceExpr_H
#define GPOPT_CScalarArrayCoerceExpr_H

#include "gpos/base.h"
#include "gpopt/operators/CScalarCoerceBase.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarArrayCoerceExpr
	//
	//	@doc:
	//		Scalar Array Coerce Expr operator
	//
	//---------------------------------------------------------------------------
	class CScalarArrayCoerceExpr : public CScalarCoerceBase
	{

		private:
			// catalog MDId of the element function
			IMDId *m_pmdidElementFunc;

			// conversion semantics flag to pass to func
			BOOL m_fIsExplicit;

			// private copy ctor
			CScalarArrayCoerceExpr(const CScalarArrayCoerceExpr &);

		public:

			// ctor
			CScalarArrayCoerceExpr
				(
				IMemoryPool *pmp,
				IMDId *pmdidElementFunc,
				IMDId *pmdidResultType,
				INT iTypeModifier,
				BOOL fIsExplicit,
				ECoercionForm edxlcf,
				INT iLoc
				);

			// dtor
			virtual
			~CScalarArrayCoerceExpr();
		
			// return metadata id of element coerce function
			IMDId *PmdidElementFunc() const;

			BOOL FIsExplicit() const;

			virtual
			EOperatorId Eopid() const;

			// return a string for operator name
			virtual
			const CHAR *SzId() const;

			// match function
			virtual
			BOOL FMatch
				(
				COperator *pop
				) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const;

			// conversion function
			static
			CScalarArrayCoerceExpr *PopConvert
				(
				COperator *pop
				);

	}; // class CScalarArrayCoerceExpr

}


#endif // !GPOPT_CScalarArrayCoerceExpr_H

// EOF
