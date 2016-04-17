//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarFunc.h
//
//	@doc:
//		Class for scalar function calls
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarFunc_H
#define GPOPT_CScalarFunc_H

#include "gpos/base.h"
#include "naucrates/md/IMDId.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

namespace gpopt
{

	using namespace gpos;
	using namespace gpmd;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarFunc
	//
	//	@doc:
	//		scalar function operator
	//
	//---------------------------------------------------------------------------
	class CScalarFunc : public CScalar
	{
		
		protected:

			// func id
			IMDId *m_pmdidFunc;

			// return type
			IMDId *m_pmdidRetType;

			// function name
			const CWStringConst *m_pstrFunc;

			// function stability
			IMDFunction::EFuncStbl m_efs;

			// function data access
			IMDFunction::EFuncDataAcc m_efda;

			// can the function return multiple rows?
			BOOL m_fReturnsSet;

			// does operator return NULL on NULL input?
			BOOL m_fReturnsNullOnNullInput;

			// is operator return type BOOL?
			BOOL m_fBoolReturnType;

		private:

			// private copy ctor
			CScalarFunc(const CScalarFunc &);


		public:
		
			// ctor
			explicit
			CScalarFunc(IMemoryPool *pmp);

			// ctor
			CScalarFunc(IMemoryPool *pmp, IMDId *pmdidFunc, IMDId *pmdidRetType, const CWStringConst *pstrFunc);

			// dtor
			virtual 
			~CScalarFunc();

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopScalarFunc;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CScalarFunc";
			}


			// operator specific hash function
			ULONG UlHash() const;
			
			// match function
			BOOL FMatch(COperator *pop) const;
			
			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const
			{
				return true;
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

			// derive function properties
			virtual
			CFunctionProp *PfpDerive
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PfpDeriveFromChildren(pmp, exprhdl, m_efs, m_efda, false /*fHasVolatileFunctionScan*/, false /*fScan*/);
			}

			// derive non-scalar function existence
			virtual
			BOOL FHasNonScalarFunction(CExpressionHandle &exprhdl);

			// conversion function
			static
			CScalarFunc *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarFunc == pop->Eopid());
				
				return reinterpret_cast<CScalarFunc*>(pop);
			}
			

			// function name
			const CWStringConst *PstrFunc() const;

			// func id
			IMDId *PmdidFunc() const;

			// the type of the scalar expression
			virtual 
			IMDId *PmdidType() const;

			// function stability
			IMDFunction::EFuncStbl EfsGetFunctionStability() const;

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber(DrgPul *pdrgpulChildren) const;

			// print
			virtual 
			IOstream &OsPrint(IOstream &os) const;


	}; // class CScalarFunc

}


#endif // !GPOPT_CScalarFunc_H

// EOF
