//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSubqNAryJoin2Apply.h
//
//	@doc:
//		Transform NAry Join to Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSubqNAryJoin2Apply_H
#define GPOPT_CXformSubqNAryJoin2Apply_H

#include "gpos/base.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/xforms/CXformSubqJoin2Apply.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformSubqNAryJoin2Apply
	//
	//	@doc:
	//		Transform NAry Join with subquery predicates to Apply
	//
	//---------------------------------------------------------------------------
	class CXformSubqNAryJoin2Apply : public CXformSubqJoin2Apply
	{

		private:

			// private copy ctor
			CXformSubqNAryJoin2Apply(const CXformSubqNAryJoin2Apply &);

		public:

			// ctor
			explicit
			CXformSubqNAryJoin2Apply
				(
				IMemoryPool *pmp
				)
				:
				CXformSubqJoin2Apply
					(
					 // pattern
					GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalNAryJoin(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp)),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))
						)
					)
			{}

			// dtor
			virtual
			~CXformSubqNAryJoin2Apply()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfSubqNAryJoin2Apply;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformSubqNAryJoin2Apply";
			}

	}; // class CXformSubqNAryJoin2Apply

}

#endif // !GPOPT_CXformSubqNAryJoin2Apply_H

// EOF
