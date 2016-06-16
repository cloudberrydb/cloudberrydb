//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTreeMap.h
//
//	@doc:
//		Map of tree components to count, rank, and unrank abstract trees;
//
//		For description of algorithm, see also:
//
//			F. Waas, C. Galindo-Legaria, "Counting, Enumerating, and 
//			Sampling of Execution Plans in a Cost-Based Query Optimizer",
//			ACM SIGMOD, 2000
//---------------------------------------------------------------------------
#ifndef GPOPT_CTreeMap_H
#define GPOPT_CTreeMap_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"


namespace gpopt
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CTreeMap
	//
	//	@doc:
	//		Lookup table for counting/unranking of trees;
	//
	//		Enables client to associate objects of type T (e.g. CGroupExpression)
	//		with a topology solely given by edges between the object. The 
	//		unranking utilizes client provided function and generates results of
	//		type R (e.g., CExpression);
	//		U is a global context accessible to recursive rehydrate calls.
	//		Pointers to objects of type U are passed through PrUnrank calls to the
	//		rehydrate function of type PrFn.
	//
	//---------------------------------------------------------------------------	
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	class CTreeMap
	{
			// array of source pointers (sources owned by 3rd party)
			typedef CDynamicPtrArray<T, CleanupNULL> DrgPt;
			
			// array of result pointers (results owned by the tree we unrank)
			typedef CDynamicPtrArray<R, CleanupRelease<R> > DrgPr;

			// generic rehydrate function
			typedef R* (*PrFn)(IMemoryPool *, T *, DrgPr *, U *);

		private:
			
			// fwd declaration
			class CTreeNode;

			// arrays of internal nodes
			typedef CDynamicPtrArray<CTreeNode, CleanupNULL> DrgPtn;
			typedef CDynamicPtrArray<DrgPtn, CleanupRelease> DrgDrgPtn;

			//---------------------------------------------------------------------------
			//	@class:
			//		STreeLink
			//
			//	@doc:
			//		Internal structure to monitor tree links for duplicate detection
			//		purposes
			//
			//---------------------------------------------------------------------------
			struct STreeLink
			{
				private:

					// parent node
					const T *m_ptParent;

					// child index
					ULONG m_ulChildIndex;

					// child node
					const T *m_ptChild;

				public:

					// ctor
					STreeLink
						(
						const T *ptParent,
						ULONG ulChildIndex,
						const T *ptChild
						)
						:
						m_ptParent(ptParent),
						m_ulChildIndex(ulChildIndex),
						m_ptChild(ptChild)
					{
						GPOS_ASSERT(NULL != ptParent);
						GPOS_ASSERT(NULL != ptChild);
					}

					// dtor
					virtual
					~STreeLink()
					{}

					// hash function
					static
					ULONG UlHash
						(
						const STreeLink *ptlink
						)
					{
						ULONG ulHashParent = pfnHash(ptlink->m_ptParent);
						ULONG ulHashChild = pfnHash(ptlink->m_ptChild);
						ULONG ulHashChildIndex = gpos::UlHash<ULONG>(&ptlink->m_ulChildIndex);

						return UlCombineHashes
								(
								ulHashParent,
								UlCombineHashes(ulHashChild, ulHashChildIndex)
								);
					}

					// equality function
					static
					BOOL FEqual
						(
						const STreeLink *ptlink1,
						const STreeLink *ptlink2
						)
					{
						return  pfnEq(ptlink1->m_ptParent, ptlink2->m_ptParent) &&
								pfnEq(ptlink1->m_ptChild, ptlink2->m_ptChild) &&
								ptlink1->m_ulChildIndex == ptlink2->m_ulChildIndex;
					}
			}; // struct STreeLink

			//---------------------------------------------------------------------------
			//	@class:
			//		CTreeNode
			//
			//	@doc:
			//		Internal structure to manage source objects and their topology
			//
			//---------------------------------------------------------------------------
			class CTreeNode
			{
				private:
					
					// state of tree node during counting alternatives
					enum ENodeState
					{
						EnsUncounted,	// counting not initiated
						EnsCounting,	// counting in progress
						EnsCounted,		// counting complete

						EnsSentinel
					};

					// memory pool
					IMemoryPool *m_pmp;
					
					// id of node
					ULONG m_ul;
					
					// element
					const T *m_pt;
					
					// array of children arrays
					DrgDrgPtn *m_pdrgdrgptn;
					
					// number of trees rooted in this node
					ULLONG m_ullCount;
					
					// number of incoming edges
					ULONG m_ulIncoming;
					
					// node state used for counting alternatives
					ENodeState m_ens;

					// total tree count for a given child
					ULLONG UllCount(ULONG ulChild);
					
					// rehydrate tree
					R* PrUnrank(IMemoryPool *pmp, PrFn pfn, U *pU,
								ULONG ulChild, ULLONG ullRank);
					
				public:
				
					// ctor
					CTreeNode(IMemoryPool *pmp, ULONG ul, const T *pt);
					
					// dtor
					~CTreeNode();
					
					// add child alternative
					void Add(ULONG ulPos, CTreeNode *ptn);
					
					// accessor
					const T *Pt() const
					{
						return m_pt;
					}
					
					// number of trees rooted in this node
					ULLONG UllCount();
					
					// check if count has been determined for this node
					BOOL FCounted() const;

					// number of incoming edges
					ULONG UlIncoming() const
					{
						return m_ulIncoming;
					}
					
					// unrank tree of a given rank with a given rehydrate function
					R* PrUnrank
						(
						IMemoryPool *pmp, 
						PrFn pfn,
						U *pU,
						ULLONG ullRank
						);

#ifdef GPOS_DEBUG

					// debug print
					IOstream &OsPrint(IOstream &);
					
#endif // GPOS_DEBUG

			};
			
			// memory pool
			IMemoryPool *m_pmp;
			
			// counter for nodes
			ULONG m_ulCountNodes;

			// counter for links
			ULONG m_ulCountLinks;
			
			// rehydrate function
			PrFn m_prfn;
			
			// universal root (internally used only)
			CTreeNode *m_ptnRoot;
						
			// map of all nodes
			typedef gpos::CHashMap<T, CTreeNode, pfnHash, pfnEq,
						CleanupNULL, CleanupDelete<CTreeNode> > TMap;
			typedef gpos::CHashMapIter<T, CTreeNode, pfnHash, pfnEq,
						CleanupNULL, CleanupDelete<CTreeNode> > TMapIter;

			// map of created links
			typedef CHashMap<STreeLink, BOOL, STreeLink::UlHash, STreeLink::FEqual,
							CleanupDelete<STreeLink>, CleanupDelete<BOOL> > LinkMap;

			TMap *m_ptmap;
			
			// map of nodes to outgoing links
			LinkMap *m_plinkmap;

			// recursive count starting in given node
			ULLONG UllCount(CTreeNode *ptn);

			// Convert to corresponding treenode, create treenode as necessary
			CTreeNode *Ptn(const T *pt);
			
			// private copy ctor
			CTreeMap(const CTreeMap &);
						
		public:
		
			// ctor
			CTreeMap(IMemoryPool *pmp, PrFn prfn);
			
			// dtor
			~CTreeMap();
			
			// insert edge as n-th child
			void Insert(const T *ptParent, ULONG ulPos, const T *ptChild);

			// insert a root node
			void InsertRoot(const T *pt);

			// count all possible combinations
			ULLONG UllCount();

			// unrank a specific tree
			R *PrUnrank(IMemoryPool *pmp, U *pU, ULLONG ullRank) const;

			// return number of nodes
			ULONG UlNodes() const
			{
				return m_ulCountNodes;
			}

			// return number of links
			ULONG UlLinks() const
			{
				return m_ulCountLinks;
			}

#ifdef GPOS_DEBUG

			// retrieve count for individual element
			ULLONG UllCount(const T* pt);
			
			// debug print of entire map
			IOstream &OsPrint(IOstream &os);

#endif // GPOS_DEBUG

	}; // class CTreeMap
	
}

// inline template functions
#include "CTreeMap.inl"

#endif // !GPOPT_CTreeMap_H

// EOF
