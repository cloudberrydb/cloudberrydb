//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarNullIf.h
//
//	@doc:
//		Scalar NullIf Operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarNullIf_H
#define GPOPT_CScalarNullIf_H

#include "gpos/base.h"
#include "gpopt/operators/CScalar.h"

namespace gpopt
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarNullIf
	//
	//	@doc:
	//		Scalar NullIf operator
	//
	//---------------------------------------------------------------------------
	class CScalarNullIf : public CScalar
	{

		private:

			// operator id
			IMDId *m_pmdidOp;

			// return type
			IMDId *m_pmdidType;

			// does operator return NULL on NULL input?
			BOOL m_fReturnsNullOnNullInput;

			// is operator return type BOOL?
			BOOL m_fBoolReturnType;

			// private copy ctor
			CScalarNullIf(const CScalarNullIf &);

		public:

			// ctor
			CScalarNullIf(IMemoryPool *pmp, IMDId *pmdidOp, IMDId *pmdidType);

			// dtor
			virtual
			~CScalarNullIf();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopScalarNullIf;
			}

			// operator id
			virtual
			IMDId *PmdidOp() const
			{
				return m_pmdidOp;
			}

			// return type
			virtual
			IMDId *PmdidType() const
			{
				return m_pmdidType;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarNullIf";
			}

			// operator specific hash function
			virtual
			ULONG UlHash() const;

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// sensitivity to order of inputs
			virtual
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

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber(DrgPul *pdrgpulChildren) const;

			// conversion function
			static
			CScalarNullIf *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarNullIf == pop->Eopid());

				return reinterpret_cast<CScalarNullIf*>(pop);
			}

	}; // class CScalarNullIf

}

#endif // !GPOPT_CScalarNullIf_H

// EOF
