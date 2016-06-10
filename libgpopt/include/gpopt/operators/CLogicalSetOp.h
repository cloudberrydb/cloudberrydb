//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalSetOp.h
//
//	@doc:
//		Base for set operations
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalSetOp_H
#define GPOS_CLogicalSetOp_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/base/CColRefSet.h"

namespace gpopt
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalSetOp
	//
	//	@doc:
	//		Base for all set operations
	//
	//---------------------------------------------------------------------------
	class CLogicalSetOp : public CLogical
	{

		protected:

			// output column array
			DrgPcr *m_pdrgpcrOutput;
			
			// input column array
			DrgDrgPcr *m_pdrgpdrgpcrInput;

			// set representation of output columns
			CColRefSet *m_pcrsOutput;

			// set representation of input columns
			DrgPcrs *m_pdrgpcrsInput;

			// private copy ctor
			CLogicalSetOp(const CLogicalSetOp &);

			// build set representation of input/output columns for faster set operations
			void BuildColumnSets(IMemoryPool *pmp);

			// output equivalence classes
			DrgPcrs *PdrgpcrsOutputEquivClasses(IMemoryPool *pmp, CExpressionHandle &exprhdl, BOOL fIntersect) const;

			// equivalence classes from one input child, mapped to output columns
			DrgPcrs *PdrgpcrsInputMapped(IMemoryPool *pmp, CExpressionHandle &exprhdl, ULONG ulChild) const;

			// constraints for a given output column from all children
			DrgPcnstr *PdrgpcnstrColumn
						(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						ULONG ulColIndex,
						ULONG ulStart
						)
						const;

			// get constraint for a given output column from a given children
			CConstraint *PcnstrColumn
						(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						ULONG ulColIndex,
						ULONG ulChild
						)
						const;

			// derive constraint property for intersect and union operators
			CPropConstraint *PpcDeriveConstraintIntersectUnion
							(
							IMemoryPool *pmp,
							CExpressionHandle &exprhdl,
							BOOL fIntersect
							)
							const;

		public:
		
			// ctor
			explicit
			CLogicalSetOp(IMemoryPool *pmp);

			CLogicalSetOp
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgOutput,
				DrgPcr *pdrgpcrLeft,
				DrgPcr *pdrgpcrRight
				);

			CLogicalSetOp
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcrOutput,
				DrgDrgPcr *pdrgpdrgpcrInput
				);

			// dtor
			virtual ~CLogicalSetOp();

			// ident accessors
			virtual 
			EOperatorId Eopid() const = 0;
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const = 0;

			// accessor of output column array
			DrgPcr *PdrgpcrOutput() const
			{
				GPOS_ASSERT(NULL != m_pdrgpcrOutput);
				return m_pdrgpcrOutput;
			}
			
			// accessor of input column array
			DrgDrgPcr *PdrgpdrgpcrInput() const
			{
				GPOS_ASSERT(NULL != m_pdrgpdrgpcrInput);
				return m_pdrgpdrgpcrInput;
			}

			// return true if we can pull projections up past this operator from its given child
			virtual
			BOOL FCanPullProjectionsUp
				(
				ULONG //ulChildIndex
				) const
			{
				return false;
			}

			// match function
			BOOL FMatch(COperator *pop) const;

			virtual
			IOstream& OsPrint(IOstream &os) const;

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *, CExpressionHandle &);
			
			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;		

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			//-------------------------------------------------------------------------------------
			// Required Relational Properties
			//-------------------------------------------------------------------------------------

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *,// pmp
				CExpressionHandle &,// exprhdl
				CColRefSet *pcrsInput,
				ULONG // ulChildIndex
				)
				const;

			// conversion function
			static
			CLogicalSetOp *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(CUtils::FLogicalSetOp(pop));

				return dynamic_cast<CLogicalSetOp *>(pop);
			}

	}; // class CLogicalSetOp

}


#endif // !GPOS_CLogicalSetOp_H

// EOF
