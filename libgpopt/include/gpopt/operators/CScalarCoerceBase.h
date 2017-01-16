//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CScalarCoerceBase.h
//
//	@doc:
//		Scalar coerce operator base class
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCoerceBase_H
#define GPOPT_CScalarCoerceBase_H

#include "gpos/base.h"
#include "gpopt/operators/CScalar.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarCoerceBase
	//
	//	@doc:
	//		Scalar coerce operator base class
	//
	//---------------------------------------------------------------------------
	class CScalarCoerceBase : public CScalar
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
			CScalarCoerceBase(const CScalarCoerceBase &);

		public:

			// ctor
			CScalarCoerceBase
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iMod,
				ECoercionForm edxlcf,
				INT iLoc
				);

			// dtor
			virtual
			~CScalarCoerceBase()
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

	}; // class CScalarCoerceBase

}


#endif // !GPOPT_CScalarCoerceBase_H

// EOF
