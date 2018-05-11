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
			CLogicalLeftAntiSemiJoin(IMemoryPool *mp);

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
				ULONG child_index
				) const
			{
				return (0 == child_index);
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *mp, CExpressionHandle &exprhdl);

			// derive not nullable output columns
			virtual
			CColRefSet *PcrsDeriveNotNull
				(
				IMemoryPool *,// mp
				CExpressionHandle &exprhdl
				)
				const
			{
				return PcrsDeriveNotNullPassThruOuter(exprhdl);
			}
				
			// dervive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *mp, CExpressionHandle &exprhdl) const;		
					
			// derive max card
			virtual
			CMaxCard MaxCard(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, //mp,
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
			CXformSet *PxfsCandidates(IMemoryPool *mp) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *mp,
						CExpressionHandle &exprhdl,
						IStatisticsArray *stats_ctxt
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
