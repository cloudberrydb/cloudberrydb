//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalNAryJoin.h
//
//	@doc:
//		N-ary inner join operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalNAryJoin_H
#define GPOS_CLogicalNAryJoin_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalJoin.h"

namespace gpopt
{	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalNAryJoin
	//
	//	@doc:
	//		N-ary inner join operator
	//
	//---------------------------------------------------------------------------
	class CLogicalNAryJoin : public CLogicalJoin
	{
		private:

			// private copy ctor
			CLogicalNAryJoin(const CLogicalNAryJoin &);

			// Indexes that help to find ON predicates for LOJs.
			// If all joins in this NAry join are inner joins, this pointer is NULL and
			// the scalar child of this NAry join contains all join predicates directly.
			// Otherwise, the scalar child is a CScalarNaryJoinPredList and this ULongPtr
			// array has as many entries as there are logical children of the NAry
			// join. For each logical child i, if that child i is part of an inner join
			// or the outer table of an LOJ, then the corresponding entry
			// (*m_lojChildPredIndexes)[i] in this array is 0 (GPOPT_ZERO_INNER_JOIN_PRED_INDEX).
			// If the logical child is the right table of an LOJ, the corresponding
			// index indicates the child index of the CScalarNAryJoinPredList
			// expression that contains the ON predicate for the LOJ.
			ULongPtrArray *m_lojChildPredIndexes;

		public:

			// ctor
			explicit
			CLogicalNAryJoin(CMemoryPool *mp);

			CLogicalNAryJoin(CMemoryPool *mp, ULongPtrArray *lojChildIndexes);

			// dtor
			virtual
			~CLogicalNAryJoin() 
			{
				CRefCount::SafeRelease(m_lojChildPredIndexes);
			}

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalNAryJoin;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalNAryJoin";
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive not nullable columns
			virtual
			CColRefSet *DeriveNotNullColumns
				(
				CMemoryPool *mp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PcrsDeriveNotNullCombineLogical(mp, exprhdl);
			}

			// derive partition consumer info
			virtual
			CPartInfo *DerivePartitionInfo
				(
				CMemoryPool *mp,
				CExpressionHandle &exprhdl
				) 
				const
			{
				return PpartinfoDeriveCombine(mp, exprhdl);
			}

			// derive max card
			virtual
			CMaxCard DeriveMaxCard(CMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *DerivePropertyConstraint
				(
				CMemoryPool *mp,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintFromPredicates(mp, exprhdl);
			}

			//-------------------------------------------------------------------------------------
			// Derived Stats
			//-------------------------------------------------------------------------------------

			// promise level for stat derivation
			virtual
			EStatPromise Esp
				(
				CExpressionHandle & // exprhdl
				)
				const
			{
				// we should use the expanded join order for stat derivation
				return EspLow;
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(CMemoryPool *mp) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalNAryJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);

				return dynamic_cast<CLogicalNAryJoin*>(pop);
			}

			BOOL
			HasOuterJoinChildren() const
			{
				return (NULL != m_lojChildPredIndexes);
			}

			BOOL
			IsInnerJoinChild(ULONG child_num) const
			{
				return (NULL == m_lojChildPredIndexes || *((*m_lojChildPredIndexes)[child_num]) == 0);
			}

			ULongPtrArray*
			GetLojChildPredIndexes() const
			{
				return m_lojChildPredIndexes;
			}

			// get the true inner join predicates, excluding predicates that use ColRefs
			// coming from non-inner joins
			CExpression*
			GetTrueInnerJoinPreds (CMemoryPool *mp, CExpressionHandle &exprhdl) const;

			virtual
			IOstream & OsPrint(IOstream &os) const;

	}; // class CLogicalNAryJoin

}


#endif // !GPOS_CLogicalNAryJoin_H

// EOF
