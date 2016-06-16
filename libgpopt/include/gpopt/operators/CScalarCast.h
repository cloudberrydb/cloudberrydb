//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarCast.h
//
//	@doc:
//		Scalar casting operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCast_H
#define GPOPT_CScalarCast_H

#include "gpos/base.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarCast
	//
	//	@doc:
	//		Scalar casting operator
	//
	//---------------------------------------------------------------------------
	class CScalarCast : public CScalar
	{

		private:

			// return type metadata id in the catalog
			IMDId *m_pmdidReturnType;

			// function to be used for casting
			IMDId *m_pmdidFunc;

			// whether or not this cast is binary coercible
			BOOL m_fBinaryCoercible;
			
			// does operator return NULL on NULL input?
			BOOL m_fReturnsNullOnNullInput;

			// is operator's return type BOOL?
			BOOL m_fBoolReturnType;

			// private copy ctor
			CScalarCast(const CScalarCast &);

		public:

			// ctor
			CScalarCast
				(
				IMemoryPool *pmp,
				IMDId *pmdidReturnType,
				IMDId *pmdidFunc,
				BOOL fBinaryCoercible
				);

			// dtor
			virtual
			~CScalarCast() 
			{
				m_pmdidFunc->Release();
				m_pmdidReturnType->Release();
			}


			// ident accessors

			// the type of the scalar expression
			virtual 
			IMDId *PmdidType() const
			{
				return m_pmdidReturnType;
			}

			// func that casts
			IMDId *PmdidFunc() const
			{
				return m_pmdidFunc;
			}

			virtual
			EOperatorId Eopid() const
			{
				return EopScalarCast;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarCast";
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

			// whether or not this cast is binary coercible
			BOOL FBinaryCoercible() const
			{
				return m_fBinaryCoercible;
			}
			

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber
				(
				DrgPul *pdrgpulChildren
				)
				const
			{
				return EberNullOnAllNullChildren(pdrgpulChildren);
			}

			// conversion function
			static
			CScalarCast *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarCast == pop->Eopid());

				return dynamic_cast<CScalarCast*>(pop);
			}

	}; // class CScalarCast

}


#endif // !GPOPT_CScalarCast_H

// EOF
