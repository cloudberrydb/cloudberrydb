//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalUnion.h
//
//	@doc:
//		Logical Union operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalUnion_H
#define GPOPT_CLogicalUnion_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSetOp.h"

namespace gpopt
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalUnion
	//
	//	@doc:
	//		union operators
	//
	//---------------------------------------------------------------------------
	class CLogicalUnion : public CLogicalSetOp
	{

		private:

			// private copy ctor
			CLogicalUnion(const CLogicalUnion &);

		public:
		
			// ctor
			explicit
			CLogicalUnion(IMemoryPool *mp);

			CLogicalUnion
				(
				IMemoryPool *mp,
				CColRefArray *pdrgpcrOutput,
				CColRef2dArray *pdrgpdrgpcrInput
				);

			// dtor
			virtual
			~CLogicalUnion();

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalUnion;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalUnion";
			}
			
			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const
			{
				return true;
			}
			
			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintIntersectUnion(mp, exprhdl, false /*fIntersect*/);
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			CXformSet *PxfsCandidates(IMemoryPool *mp) const;

			//-------------------------------------------------------------------------------------
			// Derived Stats
			//-------------------------------------------------------------------------------------

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

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
			CLogicalUnion *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalUnion == pop->Eopid());
				
				return reinterpret_cast<CLogicalUnion*>(pop);
			}
		
	}; // class CLogicalUnion

}


#endif // !GPOPT_CLogicalUnion_H

// EOF
