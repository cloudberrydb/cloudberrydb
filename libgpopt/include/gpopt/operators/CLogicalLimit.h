//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLimit.h
//
//	@doc:
//		Limit operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLimit_H
#define GPOPT_CLogicalLimit_H

#include "gpos/base.h"

#include "naucrates/md/IMDId.h"

#include "gpopt/base/COrderSpec.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalLimit
	//
	//	@doc:
	//		Limit operator;
	//		Scalar children compute (1) offset of start row, (2) number of rows
	//
	//---------------------------------------------------------------------------
	class CLogicalLimit : public CLogical
	{
		private:
		
			// required sort order
			COrderSpec *m_pos;
			
			// global limit
			BOOL m_fGlobal;

			// does limit specify a number of rows?
			BOOL m_fHasCount;

			// the limit must be kept, even if it has no offset, nor count
			BOOL m_fTopLimitUnderDML;

			// private copy ctor
			CLogicalLimit(const CLogicalLimit &);

		public:

			// ctors
			explicit
			CLogicalLimit(IMemoryPool *pmp);
			CLogicalLimit
				(
				IMemoryPool *pmp,
				COrderSpec *pos,
				BOOL fGlobal,
				BOOL fHasCount,
				BOOL fTopLimitUnderDML
				);

			// dtor
			virtual ~CLogicalLimit();

			// ident accessors
			virtual 
			EOperatorId Eopid() const 
			{
				return EopLogicalLimit;
			}
			
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalLimit";
			}
			
			// order spec
			COrderSpec *Pos() const
			{
				return m_pos;
			}
			
			// global limit
			BOOL FGlobal() const
			{
				return m_fGlobal;
			}

			// does limit specify a number of rows
			BOOL FHasCount() const
			{
				return m_fHasCount;
			}

			// must the limit be always kept
			BOOL FTopLimitUnderDML() const
			{
				return m_fTopLimitUnderDML;
			}

			// match function
			virtual
			BOOL FMatch(COperator *) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// hash function
			virtual
			ULONG UlHash() const;

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const;

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool * pmp,CExpressionHandle &exprhdl);
				
			// derive outer references
			virtual
			CColRefSet *PcrsDeriveOuter(IMemoryPool *pmp, CExpressionHandle &exprhdl);

			// dervive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;			
			
			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *, // pmp
				CExpressionHandle &exprhdl
				) 
				const
			{
				return PpartinfoPassThruOuter(exprhdl);
			}
			
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
			// Required Relational Properties
			//-------------------------------------------------------------------------------------

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat(IMemoryPool *pmp, CExpressionHandle &exprhdl, CColRefSet *pcrsInput, ULONG ulChildIndex) const;
			
			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *) const;

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
			CLogicalLimit *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalLimit == pop->Eopid());
				
				return dynamic_cast<CLogicalLimit*>(pop);
			}

	}; // class CLogicalLimit

}

#endif // !GPOPT_CLogicalLimit_H

// EOF
