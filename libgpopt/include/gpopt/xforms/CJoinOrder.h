//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJoinOrder.h
//
//	@doc:
//		Join Order Generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrder_H
#define GPOPT_CJoinOrder_H

#include "gpos/base.h"
#include "gpos/io/IOstream.h"

namespace gpopt
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CJoinOrder
	//
	//	@doc:
	//		Helper class for creating compact join orders
	//
	//---------------------------------------------------------------------------
	class CJoinOrder
	{
		public:

			//---------------------------------------------------------------------------
			//	@struct:
			//		SEdge
			//
			//	@doc:
			//		Struct to capture edge
			//
			//---------------------------------------------------------------------------
			struct SEdge : public CRefCount
			{
				// cover of edge
				CBitSet *m_pbs;
				
				// associated conjunct
				CExpression *m_pexpr;
				
				// a flag to mark edge as used
				BOOL m_fUsed;

				// ctor
				SEdge(IMemoryPool *pmp, CExpression *pexpr);
				
				// dtor
				~SEdge();
				
				// print routine
				IOstream &OsPrint(IOstream &os) const;
			};
			
		
			//---------------------------------------------------------------------------
			//	@struct:
			//		SComponent
			//
			//	@doc:
			//		Struct to capture component
			//
			//---------------------------------------------------------------------------
			struct SComponent : public CRefCount
			{
				// cover
				CBitSet *m_pbs;
				
				// associated expression
				CExpression *m_pexpr;
				
				// a flag to component edge as used
				BOOL m_fUsed;

				// ctor
				SComponent(IMemoryPool *pmp, CExpression *pexpr);
				
				// ctor
				SComponent(CExpression *pexpr, CBitSet *pbs);

				// dtor
				~SComponent();

				// print routine
				IOstream &OsPrint(IOstream &os) const;
			};

		protected:
				
			// memory pool
			IMemoryPool *m_pmp;
			
			// edges
			SEdge **m_rgpedge;
			
			// number of edges
			ULONG m_ulEdges;
			
			// components
			SComponent **m_rgpcomp;
			
			// number of components
			ULONG m_ulComps;
			
			// compute cover of each edge
			void ComputeEdgeCover();
			
			// apply edges to graph components
			void MergeComponents();
			
			// add edge and merge associated components
			void AddEdge(SEdge *pedge, DrgPexpr *pdrgpexprConjuncts);

			// combine two disjoint components
			void CombineComponents(SComponent *pcompLeft, SComponent *pcompRight, DrgPexpr *pdrgpexpr);
			
			// determine order in which to apply edges
			virtual 
			void SortEdges();

		private:

			// private copy ctor
			CJoinOrder(const CJoinOrder &);

		public:

			// ctor
			CJoinOrder
				(
				IMemoryPool *pmp,
				DrgPexpr *pdrgpexprComponents,
				DrgPexpr *pdrgpexprConjuncts
				);
		
			// dtor
			virtual
			~CJoinOrder();			
		
			// main handler
			virtual
			CExpression *PexprExpand();
			
			// print function
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CJoinOrder

}

#endif // !GPOPT_CJoinOrder_H

// EOF
