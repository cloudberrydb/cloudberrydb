//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CScalarArrayRef.h
//
//	@doc:
//		Class for scalar arrayref
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarArrayRef_H
#define GPOPT_CScalarArrayRef_H

#include "gpos/base.h"
#include "naucrates/md/IMDId.h"

#include "gpopt/operators/CScalar.h"

namespace gpopt
{

	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarArrayRef
	//
	//	@doc:
	//		Scalar arrayref
	//
	//		Arrayrefs are used to reference array elements or subarrays
	//		e.g. select a[1], b[1][2][3], c[3:5], d[1:2][4:9] from arrtest;
	//
	//---------------------------------------------------------------------------
	class CScalarArrayRef : public CScalar
	{
		private:

			// element type id
			IMDId *m_pmdidElem;

			// element type modifier
			INT m_iTypeModifier;

			// array type id
			IMDId *m_pmdidArray;

			// return type id
			IMDId *m_pmdidType;

			// private copy ctor
			CScalarArrayRef(const CScalarArrayRef &);

		public:

			// ctor
			CScalarArrayRef(IMemoryPool *pmp, IMDId *pmdidElem, INT iTypeModifier, IMDId *pmdidArray, IMDId *pmdidReturn);

			// dtor
			virtual
			~CScalarArrayRef();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopScalarArrayRef;
			}

			// operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarArrayRef";
			}

			// element type id
			IMDId *PmdidElem() const
			{
				return m_pmdidElem;
			}

			// element type modifier
			virtual INT ITypeModifier() const;

			// array type id
			IMDId *PmdidArray() const
			{
				return m_pmdidArray;
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

			// type of expression's result
			virtual
			IMDId *PmdidType() const
			{
				return m_pmdidType;
			}

			// conversion function
			static
			CScalarArrayRef *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarArrayRef == pop->Eopid());

				return dynamic_cast<CScalarArrayRef*>(pop);
			}

	}; // class CScalarArrayRef
}

#endif // !GPOPT_CScalarArrayRef_H

// EOF
