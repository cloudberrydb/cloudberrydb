//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarSwitchCase.h
//
//	@doc:
//		Scalar SwitchCase operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarSwitchCase_H
#define GPOPT_CScalarSwitchCase_H

#include "gpos/base.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/base/CDrvdProp.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarSwitchCase
	//
	//	@doc:
	//		Scalar SwitchCase operator
	//
	//---------------------------------------------------------------------------
	class CScalarSwitchCase : public CScalar
	{

		private:

			// private copy ctor
			CScalarSwitchCase(const CScalarSwitchCase &);

		public:

			// ctor
			explicit
			CScalarSwitchCase(IMemoryPool *pmp);

			// dtor
			virtual
			~CScalarSwitchCase()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopScalarSwitchCase;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarSwitchCase";
			}

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

			virtual
			IMDId *PmdidType() const
			{
				GPOS_ASSERT(!"Invalid function call: CScalarSwitchCase::PmdidType()");
				return NULL;
			}

			// boolean expression evaluation
			virtual
			EBoolEvalResult Eber
				(
				DrgPul *pdrgpulChildren
				)
				const
			{
				return EberNullOnAllNullChildren(pdrgpulChildren);
			}

			// conversion function
			static
			CScalarSwitchCase *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarSwitchCase == pop->Eopid());

				return dynamic_cast<CScalarSwitchCase*>(pop);
			}

	}; // class CScalarSwitchCase

}


#endif // !GPOPT_CScalarSwitchCase_H

// EOF
