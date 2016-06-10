//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarOp.h
//
//	@doc:
//		Base class for all scalar operations such as arithmetic and string
//		evaluations
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarOp_H
#define GPOPT_CScalarOp_H

#include "gpos/base.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

#include "naucrates/md/IMDId.h"

namespace gpopt
{

	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarOp
	//
	//	@doc:
	//		general scalar operation such as arithmetic and string evaluations
	//
	//---------------------------------------------------------------------------
	class CScalarOp : public CScalar
	{

		private:

			// metadata id in the catalog
			IMDId *m_pmdidOp;

			// return type id or NULL if it can be inferred from the metadata
			IMDId *m_pmdidReturnType;
			
			// scalar operator name
			const CWStringConst *m_pstrOp;

			// does operator return NULL on NULL input?
			BOOL m_fReturnsNullOnNullInput;

			// is operator return type BOOL?
			BOOL m_fBoolReturnType;

			// is operator commutative
			BOOL m_fCommutative;

			// private copy ctor
			CScalarOp(const CScalarOp &);

		public:

			// ctor
			CScalarOp
				(
				IMemoryPool *pmp,
				IMDId *pmdidOp,
				IMDId *pmdidReturnType,
				const CWStringConst *pstrOp
				);

			// dtor
			virtual
			~CScalarOp()
			{
				m_pmdidOp->Release();
				CRefCount::SafeRelease(m_pmdidReturnType);
				GPOS_DELETE(m_pstrOp);
			}


			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopScalarOp;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarOp";
			}
			
			// accessor to the return type field
			IMDId *PmdidReturnType() const;

			// the type of the scalar expression
			virtual 
			IMDId *PmdidType() const;

			// operator specific hash function
			ULONG UlHash() const;

			// match function
			BOOL FMatch(COperator *pop) const;

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const;

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
			CScalarOp *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarOp == pop->Eopid());

				return reinterpret_cast<CScalarOp*>(pop);
			}

			// helper function
			static 
			BOOL FCommutative(const IMDId *pcmdidOtherOp);

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber(DrgPul *pdrgpulChildren) const;

			// name of the scalar operator
			const CWStringConst *Pstr() const;

			// metadata id
			IMDId *PmdidOp() const;

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const;

	}; // class CScalarOp

}

#endif // !GPOPT_CScalarOp_H

// EOF
