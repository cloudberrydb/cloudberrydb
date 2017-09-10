//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarWindowFunc.h
//
//	@doc:
//		Class for scalar window function
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarWindowFunc_H
#define GPOPT_CScalarWindowFunc_H

#include "gpos/base.h"
#include "naucrates/md/IMDId.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalarFunc.h"
#include "gpopt/base/CDrvdProp.h"

namespace gpopt
{

	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScalarWindowFunc
	//
	//	@doc:
	//		Class for scalar window function
	//
	//---------------------------------------------------------------------------
	class CScalarWindowFunc : public CScalarFunc
	{
		public:
			// window stage
			enum EWinStage
			{
				EwsImmediate,
				EwsPreliminary,
				EwsRowKey,

				EwsSentinel
			};

		private:

			// window stage
			EWinStage m_ewinstage;

			// distinct window computation
			BOOL m_fDistinct;

			/* TRUE if argument list was really '*' */
			BOOL m_fStarArg;

			/* is function a simple aggregate? */
			BOOL m_fSimpleAgg;

			// aggregate window function, e.g. count(*) over()
			BOOL m_fAgg;

			// private copy ctor
			CScalarWindowFunc(const CScalarWindowFunc &);

		public:

			// ctor
			CScalarWindowFunc
				(
				IMemoryPool *pmp,
				IMDId *pmdidFunc,
				IMDId *pmdidRetType,
				const CWStringConst *pstrFunc,
				EWinStage ewinstage,
				BOOL fDistinct,
				BOOL fStarArg,
				BOOL fSimpleAgg
				);

			// dtor
			virtual
			~CScalarWindowFunc(){}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopScalarWindowFunc;
			}

			// return a string for window function
			virtual
			const CHAR *SzId() const
			{
				return "CScalarWindowFunc";
			}

			EWinStage Ews() const
			{
				return m_ewinstage;
			}

			// operator specific hash function
			ULONG UlHash() const;

			// match function
			BOOL FMatch(COperator *pop) const;

			// conversion function
			static
			CScalarWindowFunc *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopScalarWindowFunc == pop->Eopid());

				return reinterpret_cast<CScalarWindowFunc*>(pop);
			}

			// does window function definition include Distinct?
			BOOL FDistinct() const
			{
				return m_fDistinct;
			}
		
			BOOL FStarArg() const
			{
				return m_fStarArg;
			}

			BOOL FSimpleAgg() const
			{
				return m_fSimpleAgg;
			}

			// is window function defined as Aggregate?
			BOOL FAgg() const
			{
				return m_fAgg;
			}

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const;


	}; // class CScalarWindowFunc

}

#endif // !GPOPT_CScalarWindowFunc_H

// EOF
