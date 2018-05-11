//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalLeftSemiApplyIn.h
//
//	@doc:
//		Logical Left Semi Apply operator used in IN/ANY subqueries
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalLeftSemiApplyIn_H
#define GPOPT_CLogicalLeftSemiApplyIn_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalLeftSemiApply.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{


	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalLeftSemiApplyIn
	//
	//	@doc:
	//		Logical Apply operator used in IN/ANY subqueries
	//
	//---------------------------------------------------------------------------
	class CLogicalLeftSemiApplyIn : public CLogicalLeftSemiApply
	{

		private:

			// private copy ctor
			CLogicalLeftSemiApplyIn(const CLogicalLeftSemiApplyIn &);

		public:

			// ctor
			explicit
			CLogicalLeftSemiApplyIn
				(
				IMemoryPool *mp
				)
				:
				CLogicalLeftSemiApply(mp)
			{}

			// ctor
			CLogicalLeftSemiApplyIn
				(
				IMemoryPool *mp,
				CColRefArray *pdrgpcrInner,
				EOperatorId eopidOriginSubq
				)
				:
				CLogicalLeftSemiApply(mp, pdrgpcrInner, eopidOriginSubq)
			{}

			// dtor
			virtual
			~CLogicalLeftSemiApplyIn()
			{}

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalLeftSemiApplyIn;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalLeftSemiApplyIn";
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *mp) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

			// conversion function
			static
			CLogicalLeftSemiApplyIn *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalLeftSemiApplyIn == pop->Eopid());

				return dynamic_cast<CLogicalLeftSemiApplyIn*>(pop);
			}

	}; // class CLogicalLeftSemiApplyIn

}


#endif // !GPOPT_CLogicalLeftSemiApplyIn_H

// EOF
