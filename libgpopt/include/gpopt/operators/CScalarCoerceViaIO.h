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
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

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
	class CScalarCoerceViaIO : public CScalar
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

			// private copy ctor
			CScalarCoerceViaIO(const CScalarCoerceViaIO &);

		public:

			// ctor
			CScalarCoerceViaIO
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iMod,
				ECoercionForm edxlcf,
				INT iLoc
				);

			// dtor
			virtual
			~CScalarCoerceViaIO()
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
