//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDifferenceAll.h
//
//	@doc:
//		Logical Difference all operator (Difference all does not remove
//		duplicates from the left child)
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalDifferenceAll_H
#define GPOPT_CLogicalDifferenceAll_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSetOp.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalDifferenceAll
	//
	//	@doc:
	//		Difference all operators
	//
	//---------------------------------------------------------------------------
	class CLogicalDifferenceAll : public CLogicalSetOp
	{

		private:

			// private copy ctor
			CLogicalDifferenceAll(const CLogicalDifferenceAll &);

		public:

			// ctor
			explicit
			CLogicalDifferenceAll(IMemoryPool *mp);

			CLogicalDifferenceAll
				(
				IMemoryPool *mp,
				CColRefArray *pdrgpcrOutput,
				CColRef2dArray *pdrgpdrgpcrInput
				);

			// dtor
			virtual
			~CLogicalDifferenceAll();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalDifferenceAll;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalDifferenceAll";
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

			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

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

			// stat promise
			virtual
			EStatPromise Esp
				(
				CExpressionHandle &  // exprhdl
				)
				const
			{
				return CLogical::EspLow;
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

			// conversion function
			static
			CLogicalDifferenceAll *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalDifferenceAll == pop->Eopid());

				return reinterpret_cast<CLogicalDifferenceAll*>(pop);
			}

	}; // class CLogicalDifferenceAll

}

#endif // !GPOPT_CLogicalDifferenceAll_H

// EOF
