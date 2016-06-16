//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CScalarCoerceToDomain.h
//
//	@doc:
//		Scalar CoerceToDomain operator,
//		the operator captures coercing a value to a domain type,
//
//		at runtime, the precise set of constraints to be checked against
//		value are determined,
//		if the value passes, it is returned as the result, otherwise an error
//		is raised.

//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCoerceToDomain_H
#define GPOPT_CScalarCoerceToDomain_H

#include "gpos/base.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarCoerceToDomain
	//
	//	@doc:
	//		Scalar CoerceToDomain operator
	//
	//---------------------------------------------------------------------------
	class CScalarCoerceToDomain : public CScalar
	{

		private:

			// catalog MDId of the result type
			IMDId *m_pmdidResultType;

			// output type modifications
			INT m_iMod;

			// coercion form
			ECoercionForm m_ecf;

			// location of token to be coerced
			INT m_iLoc;

			// does operator return NULL on NULL input?
			BOOL m_fReturnsNullOnNullInput;

			// private copy ctor
			CScalarCoerceToDomain(const CScalarCoerceToDomain &);

		public:

			// ctor
			CScalarCoerceToDomain
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iMod,
				ECoercionForm edxlcf,
				INT iLoc
				);

			// dtor
			virtual
			~CScalarCoerceToDomain()
			{
				m_pmdidResultType->Release();
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
				return EopScalarCoerceToDomain;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarCoerceToDomain";
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

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber(DrgPul *pdrgpulChildren) const;

			// conversion function
			static
			CScalarCoerceToDomain *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarCoerceToDomain == pop->Eopid());

				return dynamic_cast<CScalarCoerceToDomain*>(pop);
			}

	}; // class CScalarCoerceToDomain

}


#endif // !GPOPT_CScalarCoerceToDomain_H

// EOF
