//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftSemiJoin.h
//
//	@doc:
//		Left semi join operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalLeftSemiJoin_H
#define GPOS_CLogicalLeftSemiJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalJoin.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalLeftSemiJoin
	//
	//	@doc:
	//		Left semi join operator
	//
	//---------------------------------------------------------------------------
	class CLogicalLeftSemiJoin : public CLogicalJoin
	{
		private:

			// private copy ctor
			CLogicalLeftSemiJoin(const CLogicalLeftSemiJoin &);

		public:

			// ctor
			explicit
			CLogicalLeftSemiJoin(IMemoryPool *pmp);

			// dtor
			virtual
			~CLogicalLeftSemiJoin()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalLeftSemiJoin;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalLeftSemiJoin";
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
			CColRefSet *PcrsDeriveOutput(IMemoryPool *pmp, CExpressionHandle &hdl);
			
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
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

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

			// promise level for stat derivation
			virtual
			EStatPromise Esp
				(
				CExpressionHandle & // exprhdl
				)
				const
			{
				// semi join can be converted to inner join, which is used for stat derivation
				return EspMedium;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalLeftSemiJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalLeftSemiJoin == pop->Eopid());

				return dynamic_cast<CLogicalLeftSemiJoin*>(pop);
			}

			// derive statistics
			static
			IStatistics *PstatsDerive
				(
				IMemoryPool *pmp,
				DrgPstatspredjoin *pdrgpstatspredjoin,
				IStatistics *pstatsOuter,
				IStatistics *pstatsInner
				);

	}; // class CLogicalLeftSemiJoin

}


#endif // !GPOS_CLogicalLeftSemiJoin_H

// EOF
