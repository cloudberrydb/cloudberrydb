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
#include "gpopt/operators/CScalarCoerceBase.h"

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
	class CScalarCoerceToDomain : public CScalarCoerceBase
	{

		private:
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
				INT iTypeModifier,
				ECoercionForm edxlcf,
				INT iLoc
				);

			// dtor
			virtual
			~CScalarCoerceToDomain()
			{
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
