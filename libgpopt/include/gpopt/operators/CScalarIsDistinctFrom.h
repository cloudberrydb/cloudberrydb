//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarIsDistinctFrom.h
//
//	@doc:
//		Is distinct from operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarIsDistinctFrom_H
#define GPOPT_CScalarIsDistinctFrom_H

#include "gpos/base.h"
#include "gpopt/operators/CScalarCmp.h"


namespace gpopt
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarIsDistinctFrom
	//
	//	@doc:
	//		Is distinct from operator
	//
	//---------------------------------------------------------------------------
	class CScalarIsDistinctFrom : public CScalarCmp
	{

		private:

			// private copy ctor
			CScalarIsDistinctFrom(const CScalarIsDistinctFrom &);

		public:

			// ctor
			CScalarIsDistinctFrom
				(
				IMemoryPool *pmp,
				IMDId *pmdidOp,
				const CWStringConst *pstrOp
				)
				:
				CScalarCmp(pmp, pmdidOp, pstrOp, IMDType::EcmptIDF)
			{
				GPOS_ASSERT(pmdidOp->FValid());
			}

			// dtor
			virtual
			~CScalarIsDistinctFrom()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopScalarIsDistinctFrom;
			}

			virtual
			EBoolEvalResult Eber(DrgPul *pdrgpulChildren) const;

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CScalarIsDistinctFrom";
			}

			virtual
			BOOL FMatch(COperator *pop) const;

			// conversion function
			static
			CScalarIsDistinctFrom *PopConvert(COperator *pop);

			// get commuted scalar IDF operator
			virtual
			CScalarIsDistinctFrom *PopCommutedOp(IMemoryPool *pmp, COperator *pop);

	}; // class CScalarIsDistinctFrom

}

#endif // !GPOPT_CScalarIsDistinctFrom_H

// EOF
