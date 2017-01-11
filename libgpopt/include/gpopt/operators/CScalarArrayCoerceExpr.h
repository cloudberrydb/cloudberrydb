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
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

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
	class CScalarArrayCoerceExpr : public CScalar
	{

		private:

			// catalog MDId of the element function
			IMDId *m_pmdidElementFunc;
		
			// catalog MDId of the result type
			IMDId *m_pmdidResultType;

			// output type modifications
			INT m_iMod;

			// conversion semantics flag to pass to func
			BOOL m_fIsExplicit;
		
			// coercion form
			ECoercionForm m_ecf;

			// location of token to be coerced
			INT m_iLoc;

			// private copy ctor
			CScalarArrayCoerceExpr(const CScalarArrayCoerceExpr &);

		public:

			// ctor
			CScalarArrayCoerceExpr
				(
				IMemoryPool *pmp,
				IMDId *pmdidElementFunc,
				IMDId *pmdidResultType,
				INT iMod,
				BOOL fIsExplicit,
				ECoercionForm edxlcf,
				INT iLoc
				);

			// dtor
			virtual
			~CScalarArrayCoerceExpr()
			{
				m_pmdidElementFunc->Release();
				m_pmdidResultType->Release();
			}
		
			// return metadata id of element coerce function
			IMDId *PmdidElementFunc() const
			{
				return m_pmdidElementFunc;
			}

			// the type of the scalar expression
			virtual
			IMDId *PmdidType() const
			{
				return m_pmdidResultType;
			}

			// return type modification
			INT IMod() const
			{
				return m_iMod;
			}
		
			BOOL FIsExplicit() const
			{
				return m_fIsExplicit;
			}

			// return coercion form
			ECoercionForm Ecf() const
			{
				return m_ecf;
			}

			// return token location
			INT ILoc() const
			{
				return m_iLoc;
			}

			virtual
			EOperatorId Eopid() const
			{
				return EopScalarArrayCoerceExpr;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarArrayCoerceExpr";
			}

			// match function
			virtual
			BOOL FMatch(COperator *) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return false;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns
				(
				IMemoryPool *, //pmp,
				HMUlCr *, //phmulcr,
				BOOL //fMustExist
				)
			{
				return PopCopyDefault();
			}

			// conversion function
			static
			CScalarArrayCoerceExpr *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarArrayCoerceExpr == pop->Eopid());

				return dynamic_cast<CScalarArrayCoerceExpr*>(pop);
			}

	}; // class CScalarArrayCoerceExpr

}


#endif // !GPOPT_CScalarArrayCoerceExpr_H

// EOF
