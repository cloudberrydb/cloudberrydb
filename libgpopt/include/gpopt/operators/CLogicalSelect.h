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

	typedef CHashMap<CExpression, CExpression, CExpression::UlHash, CUtils::FEqual,
		CleanupRelease<CExpression>, CleanupRelease<CExpression> > HMPexprPartPred;

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

			HMPexprPartPred *m_phmPexprPartPred;

		public:

			// ctor
			explicit
			CLogicalSelect(IMemoryPool *pmp);

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
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;		
					
			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *pmp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintFromPredicates(pmp, exprhdl);
			}

			// compute partition predicate to pass down to n-th child
			virtual
			CExpression *PexprPartPred
							(
							IMemoryPool *pmp,
							CExpressionHandle &exprhdl,
							CExpression *pexprInput,
							ULONG ulChildIndex
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
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						DrgPstat *pdrgpstatCtxt
						)
						const;

	}; // class CLogicalSelect

}

#endif // !GPOS_CLogicalSelect_H

// EOF
