//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftAntiSemiJoin.h
//
//	@doc:
//		Left anti semi join operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalLeftAntiSemiJoin_H
#define GPOS_CLogicalLeftAntiSemiJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalJoin.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalLeftAntiSemiJoin
	//
	//	@doc:
	//		Left anti semi join operator
	//
	//---------------------------------------------------------------------------
	class CLogicalLeftAntiSemiJoin : public CLogicalJoin
	{
		private:

			// private copy ctor
			CLogicalLeftAntiSemiJoin(const CLogicalLeftAntiSemiJoin &);

		public:

			// ctor
			explicit
			CLogicalLeftAntiSemiJoin(IMemoryPool *pmp);

			// dtor
			virtual
			~CLogicalLeftAntiSemiJoin()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalLeftAntiSemiJoin;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalLeftAntiSemiJoin";
			}

			// return true if we can pull projections up past this operator from its given child
			virtual
			BOOL FCanPullProjectionsUp
				(
				ULONG ulChildIndex
				) const
			{
				return (0 == ulChildIndex);
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// derive not nullable output columns
			virtual
			CColRefSet *PcrsDeriveNotNull
				(
				IMemoryPool *,// pmp
				CExpressionHandle &exprhdl
				)
				const
			{
				return PcrsDeriveNotNullPassThruOuter(exprhdl);
			}
				
			// dervive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;		
					
			// derive max card
			virtual
			CMaxCard MaxCard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, //pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						DrgPstat *pdrgpstatCtxt
						)
						const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalLeftAntiSemiJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalLeftAntiSemiJoin == pop->Eopid());

				return dynamic_cast<CLogicalLeftAntiSemiJoin*>(pop);
			}

	}; // class CLogicalLeftAntiSemiJoin

}


#endif // !GPOS_CLogicalLeftAntiSemiJoin_H

// EOF
