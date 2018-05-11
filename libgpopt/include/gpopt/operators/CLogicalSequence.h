//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSequence.h
//
//	@doc:
//		Logical sequence operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalSequence_H
#define GPOPT_CLogicalSequence_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalSequence
	//
	//	@doc:
	//		Logical sequence operator
	//
	//---------------------------------------------------------------------------
	class CLogicalSequence : public CLogical
	{
		private:

			// private copy ctor
			CLogicalSequence(const CLogicalSequence &);

		public:

			// ctor
			explicit
			CLogicalSequence(IMemoryPool *mp);

			// dtor
			virtual
			~CLogicalSequence() 
			{}

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalSequence;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalSequence";
			}
		
			// match function
			virtual
			BOOL Matches(COperator *pop) const;


			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns
						(
						IMemoryPool *, //mp,
						UlongToColRefMap *, //colref_mapping,
						BOOL //must_exist
						)
			{
				return PopCopyDefault();
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *mp, CExpressionHandle &exprhdl);

			// dervive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, //mp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintPassThru(exprhdl, exprhdl.Arity() - 1);
			}

			//-------------------------------------------------------------------------------------
			// Required Relational Properties
			//-------------------------------------------------------------------------------------

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsInput,
				ULONG child_index
				)
				const
			{
				const ULONG ulLastChildIndex = exprhdl.Arity() - 1;
				if (child_index == ulLastChildIndex)
				{
					// only pass through the required stats column to the last child since
					// the output of the sequence operator is the output of the last child
					return PcrsStatsPassThru(pcrsInput);
				}

				return GPOS_NEW(mp) CColRefSet(mp);
			}

			// derive statistics
			virtual
			IStatistics *PstatsDerive
				(
				IMemoryPool *, //mp,
				CExpressionHandle &exprhdl,
				IStatisticsArray * //stats_ctxt
				)
				const
			{
				// pass through stats from last child
				IStatistics *stats = exprhdl.Pstats(exprhdl.Arity() - 1);
				stats->AddRef();

				return stats;
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *mp) const;

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalSequence *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalSequence == pop->Eopid());
				
				return dynamic_cast<CLogicalSequence*>(pop);
			}

	}; // class CLogicalSequence

}


#endif // !GPOPT_CLogicalSequence_H

// EOF
