//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarCmp.h
//
//	@doc:
//		Base class for all ScalarCmp operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCmp_H
#define GPOPT_CScalarCmp_H

#include "gpos/base.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

namespace gpopt
{

	using namespace gpos;
	using namespace gpmd;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarCmp
	//
	//	@doc:
	//		scalar comparison operator
	//
	//---------------------------------------------------------------------------
	class CScalarCmp : public CScalar
	{
		private:

			// metadata id in the catalog
			IMDId *m_pmdidOp;

			// comparison operator name
			const CWStringConst *m_pstrOp;
			
			// comparison type
			IMDType::ECmpType m_ecmpt;

			// does operator return NULL on NULL input?
			BOOL m_fReturnsNullOnNullInput;

			// is comparison commutative
			BOOL m_fCommutative;

			// private copy ctor
			CScalarCmp(const CScalarCmp &);

		public:
		
			// ctor
			CScalarCmp
				(
				IMemoryPool *pmp,
				IMDId *pmdidOp,
				const CWStringConst *pstrOp,
				IMDType::ECmpType ecmpt
				);

			// dtor
			virtual 
			~CScalarCmp()
			{
				m_pmdidOp->Release();
				GPOS_DELETE(m_pstrOp);
			}


			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopScalarCmp;
			}
			
			// comparison type
			IMDType::ECmpType Ecmpt() const
			{
				return m_ecmpt;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CScalarCmp";
			}


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

			// is operator commutative
			BOOL FCommutative() const;

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber(DrgPul *pdrgpulChildren) const;

			// name of the comparison operator
			const CWStringConst *Pstr() const;

			// metadata id
			IMDId *PmdidOp() const;
			
			// the type of the scalar expression
			virtual 
			IMDId *PmdidType() const;

			// print
			virtual 
			IOstream &OsPrint(IOstream &os) const;

			// conversion function
			static
			CScalarCmp *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarCmp == pop->Eopid());

				return dynamic_cast<CScalarCmp*>(pop);
			}
		
			// get commuted scalar comparision operator
			virtual
			CScalarCmp *PopCommutedOp(IMemoryPool *pmp, COperator *pop);
		
			// get the string representation of a metadata object
			static
			CWStringConst *Pstr(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdid);

			// get metadata id of the commuted operator
			static
			IMDId *PmdidCommuteOp(CMDAccessor *pmda, COperator *pop);
		
		

	}; // class CScalarCmp

}

#endif // !GPOPT_CScalarCmp_H

// EOF
