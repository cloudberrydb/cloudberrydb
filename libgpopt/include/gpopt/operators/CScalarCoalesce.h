//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarCoalesce.h
//
//	@doc:
//		Scalar coalesce operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCoalesce_H
#define GPOPT_CScalarCoalesce_H

#include "gpos/base.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarCoalesce
	//
	//	@doc:
	//		Scalar coalesce operator
	//
	//---------------------------------------------------------------------------
	class CScalarCoalesce : public CScalar
	{

		private:

			// return type
			IMDId *m_pmdidType;

			// is operator return type BOOL?
			BOOL m_fBoolReturnType;

			// private copy ctor
			CScalarCoalesce(const CScalarCoalesce &);

		public:

			// ctor
			CScalarCoalesce(IMemoryPool *pmp, IMDId *pmdidType);

			// dtor
			virtual
			~CScalarCoalesce();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopScalarCoalesce;
			}

			// operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarCoalesce";
			}

			// return type
			virtual
			IMDId *PmdidType() const
			{
				return m_pmdidType;
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
			EBoolEvalResult Eber
				(
				DrgPul *pdrgpulChildren
				)
				const
			{
				// Coalesce returns the first not-null child,
				// if all children are Null, then Coalesce must return Null
				return EberNullOnAllNullChildren(pdrgpulChildren);
			}

			// conversion function
			static
			CScalarCoalesce *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarCoalesce == pop->Eopid());

				return dynamic_cast<CScalarCoalesce*>(pop);
			}

	}; // class CScalarCoalesce

}

#endif // !GPOPT_CScalarCoalesce_H

// EOF
