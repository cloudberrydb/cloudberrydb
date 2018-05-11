//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalSelect.h
//
//	@doc:
//		Select operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalSelect_H
#define GPOS_CLogicalSelect_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{

	typedef CHashMap<CExpression, CExpression, CExpression::HashValue, CUtils::Equals,
		CleanupRelease<CExpression>, CleanupRelease<CExpression> > ExprPredToExprPredPartMap;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalSelect
	//
	//	@doc:
	//		Select operator
	//
	//---------------------------------------------------------------------------
	class CLogicalSelect : public CLogicalUnary
	{
		private:

			// private copy ctor
			CLogicalSelect(const CLogicalSelect &);

			ExprPredToExprPredPartMap *m_phmPexprPartPred;

		public:

			// ctor
			explicit
			CLogicalSelect(IMemoryPool *mp);

			// dtor
			virtual
			~CLogicalSelect();

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalSelect;
			}
			
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalSelect";
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *,CExpressionHandle &);
			
			// dervive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *mp, CExpressionHandle &exprhdl) const;		
					
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
				return PpcDeriveConstraintFromPredicates(mp, exprhdl);
			}

			// compute partition predicate to pass down to n-th child
			virtual
			CExpression *PexprPartPred
							(
							IMemoryPool *mp,
							CExpressionHandle &exprhdl,
							CExpression *pexprInput,
							ULONG child_index
							)
							const;

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// return true if operator can select a subset of input tuples based on some predicate,
			virtual
			BOOL FSelectionOp() const
			{
				return true;
			}

			// conversion function
			static
			CLogicalSelect *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalSelect == pop->Eopid());
				
				return reinterpret_cast<CLogicalSelect*>(pop);
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

	}; // class CLogicalSelect

}

#endif // !GPOS_CLogicalSelect_H

// EOF
