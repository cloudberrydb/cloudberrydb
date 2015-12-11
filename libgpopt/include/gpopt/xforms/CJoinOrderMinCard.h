//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CJoinOrderMinCard.h
//
//	@doc:
//		Cardinality-based join order generation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderMinCard_H
#define GPOPT_CJoinOrderMinCard_H

#include "gpos/base.h"
#include "gpos/io/IOstream.h"
#include "gpopt/xforms/CJoinOrder.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CJoinOrderMinCard
	//
	//	@doc:
	//		Helper class for creating join orders based on cardinality of results
	//
	//---------------------------------------------------------------------------
	class CJoinOrderMinCard : public CJoinOrder
	{

		private:

			// result component
			SComponent *m_pcompResult;

			// combine the two given components using applicable edges
			SComponent *PcompCombine(SComponent *pcompOuter, SComponent *pcompInner);

			// mark edges used by result component
			void MarkUsedEdges();

			// derive stats on a given component
			static
			void DeriveStats(IMemoryPool *pmp, SComponent *pcomp);

		public:

			// ctor
			CJoinOrderMinCard
				(
				IMemoryPool *pmp,
				DrgPexpr *pdrgpexprComponents,
				DrgPexpr *pdrgpexprConjuncts
				);

			// dtor
			virtual
			~CJoinOrderMinCard();

			// main handler
			virtual
			CExpression *PexprExpand();

			// print function
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CJoinOrderMinCard

}

#endif // !GPOPT_CJoinOrderMinCard_H

// EOF
