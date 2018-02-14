//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CScalarCoerceViaIO.h
//
//	@doc:
//		Scalar CoerceViaIO operator,
//		the operator captures coercing a value from one type to another, by
//		calling the output function of the argument type, and passing the
//		result to the input function of the result type.
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCoerceViaIO_H
#define GPOPT_CScalarCoerceViaIO_H

#include "gpos/base.h"
#include "gpopt/operators/CScalarCoerceBase.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarCoerceViaIO
	//
	//	@doc:
	//		Scalar CoerceViaIO operator
	//
	//---------------------------------------------------------------------------
	class CScalarCoerceViaIO : public CScalarCoerceBase
	{

		private:
			// private copy ctor
			CScalarCoerceViaIO(const CScalarCoerceViaIO &);

		public:

			// ctor
			CScalarCoerceViaIO
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iTypeModifier,
				ECoercionForm edxlcf,
				INT iLoc
				);

			// dtor
			virtual
			~CScalarCoerceViaIO()
			{
			}

			virtual
			EOperatorId Eopid() const
			{
				return EopScalarCoerceViaIO;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarCoerceViaIO";
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

			// conversion function
			static
			CScalarCoerceViaIO *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarCoerceViaIO == pop->Eopid());

				return dynamic_cast<CScalarCoerceViaIO*>(pop);
			}

	}; // class CScalarCoerceViaIO

}


#endif // !GPOPT_CScalarCoerceViaIO_H

// EOF
