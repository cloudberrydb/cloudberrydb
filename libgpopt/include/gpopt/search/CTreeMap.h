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
					ULLONG UllCount(ULONG ulChild)
                    {
                        GPOS_CHECK_STACK_SIZE;

                        ULLONG ull = 0;

                        ULONG ulCandidates = (*m_pdrgdrgptn)[ulChild]->UlLength();
                        for (ULONG ulAlt = 0; ulAlt < ulCandidates; ulAlt++)
                        {
                            CTreeNode *ptn = (*(*m_pdrgdrgptn)[ulChild])[ulAlt];
                            ULLONG ullCount = ptn->UllCount();
                            ull = gpos::UllAdd(ull, ullCount);
                        }

                        return ull;
                    }
					
					// rehydrate tree
					R* PrUnrank(IMemoryPool *pmp, PrFn prfn, U *pU,
								ULONG ulChild, ULLONG ullRank)
                    {
                        GPOS_CHECK_STACK_SIZE;
                        GPOS_ASSERT(ullRank < UllCount(ulChild));

                        DrgPtn *pdrgptn = (*m_pdrgdrgptn)[ulChild];
                        ULONG ulCandidates = pdrgptn->UlLength();

                        CTreeNode *ptn = NULL;

                        for (ULONG ul = 0; ul < ulCandidates; ul++)
                        {
                            ptn = (*pdrgptn)[ul];
                            ULLONG ullLocalCount = ptn->UllCount();

                            if (ullRank < ullLocalCount)
                            {
                                // ullRank is now local rank for the child
                                break;
                            }

                            ullRank -= ullLocalCount;
                        }

                        GPOS_ASSERT(NULL != ptn);
                        return ptn->PrUnrank(pmp, prfn, pU, ullRank);
                    }
					
				public:
				
					// ctor
					CTreeNode(IMemoryPool *pmp, ULONG ul, const T *pt)
                        :
                        m_pmp(pmp),
                        m_ul(ul),
                        m_pt(pt),
                        m_pdrgdrgptn(NULL),
                        m_ullCount(gpos::ullong_max),
                        m_ulIncoming(0),
                        m_ens(EnsUncounted)
                    {
                        m_pdrgdrgptn = GPOS_NEW(pmp) DrgDrgPtn(pmp);
                    }
					
					// dtor
					~CTreeNode()
                    {
                        m_pdrgdrgptn->Release();
                    }
					
					// add child alternative
					void Add(ULONG ulPos, CTreeNode *ptn)
                    {
                        GPOS_ASSERT(!FCounted() && "Adding edges after counting not meaningful");

                        // insert any child arrays skipped so far; make sure we have a dense
                        // array up to the position of ulPos
                        ULONG ulLength = m_pdrgdrgptn->UlLength();
                        for (ULONG ul = ulLength; ul <= ulPos; ul++)
                        {
                            DrgPtn *pdrg = GPOS_NEW(m_pmp) DrgPtn(m_pmp);
                            m_pdrgdrgptn->Append(pdrg);
                        }

                        // increment count of incoming edges
                        ptn->m_ulIncoming++;

                        // insert to appropriate array
                        DrgPtn *pdrg = (*m_pdrgdrgptn)[ulPos];
                        GPOS_ASSERT(NULL != pdrg);		
                        pdrg->Append(ptn);
                    }
					
					// accessor
					const T *Pt() const
					{
						return m_pt;
					}
					
					// number of trees rooted in this node
					ULLONG UllCount()
                    {
                        GPOS_CHECK_STACK_SIZE;

                        GPOS_ASSERT(EnsCounting != m_ens  && "cycle in graph detected");

                        if (!FCounted())
                        {
                            // initiate counting on current node
                            m_ens = EnsCounting;

                            ULLONG ullCount = 1;

                            ULONG ulArity = m_pdrgdrgptn->UlLength();
                            for (ULONG ulChild = 0; ulChild < ulArity; ulChild++)
                            {
                                ULLONG ull = UllCount(ulChild);
                                if (0 == ull)
                                {
                                    // if current child has no alternatives, the parent cannot have alternatives
                                    ullCount = 0;
                                    break;
                                }

                                // otherwise, multiply number of child alternatives by current count
                                ullCount = gpos::UllMultiply(ullCount, ull);				
                            }

                            // counting is complete
                            m_ullCount = ullCount;
                            m_ens = EnsCounted;
                        }

                        return m_ullCount;
                    }
					
					// check if count has been determined for this node
					BOOL FCounted() const
                    {
                        return (EnsCounted == m_ens);
                    }

					// number of incoming edges
					ULONG UlIncoming() const
					{
						return m_ulIncoming;
					}
					
					// unrank tree of a given rank with a given rehydrate function
					R* PrUnrank
						(
						IMemoryPool *pmp, 
						PrFn prfn,
						U *pU,
						ULLONG ullRank
						)
                    {
                        GPOS_CHECK_STACK_SIZE;

                        R *pr = NULL;

                        if (0 == this->m_ul)
                        {
                            // global root, just unrank 0-th child
                            pr = PrUnrank(pmp, prfn, pU, 0 /* ulChild */, ullRank);
                        }
                        else
                        {
                            DrgPr *pdrg = GPOS_NEW(pmp) DrgPr(pmp);

                            ULLONG ullRankRem = ullRank;

                            ULONG ulChildren = m_pdrgdrgptn->UlLength();
                            for (ULONG ulChild = 0; ulChild < ulChildren; ulChild++)
                            {
                                ULLONG ullLocalCount = UllCount(ulChild);
                                GPOS_ASSERT(0 < ullLocalCount);
                                ULLONG ullLocalRank = ullRankRem % ullLocalCount;

                                pdrg->Append(PrUnrank(pmp, prfn, pU, ulChild, ullLocalRank));

                                ullRankRem /= ullLocalCount;
                            }

                            pr = prfn(pmp, const_cast<T*>(this->Pt()), pdrg, pU);
                        }

                        return pr;
                    }

#ifdef GPOS_DEBUG

					// debug print
					IOstream &OsPrint(IOstream &os)
                    {
                        ULONG ulChildren = m_pdrgdrgptn->UlLength();

                        os
                        << "=== Node " << m_ul << " [" << *Pt() << "] ===" << std::endl
                        << "# children: " << ulChildren << std::endl
                        << "# count: " << this->UllCount() << std::endl;

                        for (ULONG ul = 0; ul < ulChildren; ul++)
                        {
                            os << "--- child: #" << ul << " ---" << std::endl;
                            ULONG ulAlt = (*m_pdrgdrgptn)[ul]->UlLength();

                            for (ULONG ulChild = 0; ulChild < ulAlt; ulChild++)
                            {
                                CTreeNode *ptn = (*(*m_pdrgdrgptn)[ul])[ulChild];
                                os
                                << "  -> " << ptn->m_ul
                                << " [" << *ptn->Pt() << "]"
                                << std::endl;
                            }
                        }

                        return os;
                    }
					
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
			CTreeNode *Ptn(const T *pt)
            {
                GPOS_ASSERT(NULL != pt);
                CTreeNode *ptn = const_cast<CTreeNode*>(m_ptmap->PtLookup(pt));

                if (NULL == ptn)
                {
                    ptn = GPOS_NEW(m_pmp) CTreeNode(m_pmp, ++m_ulCountNodes, pt);
                    (void) m_ptmap->FInsert(const_cast<T*>(pt), ptn);
                }

                return ptn;
            }
			
			// private copy ctor
			CTreeMap(const CTreeMap &);
						
		public:
		
			// ctor
			CTreeMap(IMemoryPool *pmp, PrFn prfn)
                :
                m_pmp(pmp),
                m_ulCountNodes(0),
                m_ulCountLinks(0),
                m_prfn(prfn),
                m_ptnRoot(NULL),
                m_ptmap(NULL),
                m_plinkmap(NULL)
            {
                GPOS_ASSERT(NULL != pmp);
                GPOS_ASSERT(NULL != prfn);

                m_ptmap = GPOS_NEW(pmp) TMap(pmp);
                m_plinkmap = GPOS_NEW(pmp) LinkMap(pmp);

                // insert dummy node as global root -- the only node with NULL payload
                m_ptnRoot = GPOS_NEW(pmp) CTreeNode(pmp, 0 /* ulCounter */, NULL /* pt */);
            }
			
			// dtor
			~CTreeMap()
            {
                m_ptmap->Release();
                m_plinkmap->Release();

                GPOS_DELETE(m_ptnRoot);
            }
			
			// insert edge as n-th child
			void Insert(const T *ptParent, ULONG ulPos, const T *ptChild)
            {
                GPOS_ASSERT(ptParent != ptChild);

                // exit function if link already exists
                STreeLink *ptlink = GPOS_NEW(m_pmp) STreeLink(ptParent, ulPos, ptChild);
                if (NULL != m_plinkmap->PtLookup(ptlink))
                {
                    GPOS_DELETE(ptlink);
                    return;
                }

                CTreeNode *ptnParent = Ptn(ptParent);
                CTreeNode *ptnChild = Ptn(ptChild);

                ptnParent->Add(ulPos, ptnChild);
                ++m_ulCountLinks;

                // add created link to links map
#ifdef GPOS_DEBUG
                BOOL fInserted =
#endif // GPOS_DEBUG
                m_plinkmap->FInsert(ptlink, GPOS_NEW(m_pmp) BOOL(true));
                GPOS_ASSERT(fInserted);		
            }

			// insert a root node
			void InsertRoot(const T *pt)
            {
                GPOS_ASSERT(NULL != pt);
                GPOS_ASSERT(NULL != m_ptnRoot);

                // add logical root as 0-th child to global root
                m_ptnRoot->Add(0 /*ulPos*/, Ptn(pt));
            }

			// count all possible combinations
			ULLONG UllCount()
            {
                // first, hookup all logical root nodes to the global root
                TMapIter mi(m_ptmap);
                ULONG ulNodes = 0;
                for (ulNodes = 0; mi.FAdvance(); ulNodes++)
                {
                    CTreeNode *ptn = const_cast<CTreeNode*>(mi.Pt());

                    if (0 == ptn->UlIncoming())
                    {
                        // add logical root as 0-th child to global root
                        m_ptnRoot->Add(0 /*ulPos*/, ptn);
                    }
                }

                // special case of empty map
                if (0 == ulNodes)
                {
                    return 0;
                }

                return m_ptnRoot->UllCount();
            }

			// unrank a specific tree
			R *PrUnrank(IMemoryPool *pmp, U *pU, ULLONG ullRank) const
            {
                return m_ptnRoot->PrUnrank(pmp, m_prfn, pU, ullRank);
            }

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
			ULLONG UllCount(const T* pt)
            {
                CTreeNode *ptn = m_ptmap->PtLookup(pt);
                GPOS_ASSERT(NULL != ptn);

                return ptn->UllCount();
            }
			
			// debug print of entire map
			IOstream &OsPrint(IOstream &os)
            {
                TMapIter mi(m_ptmap);
                ULONG ulNodes = 0;
                for (ulNodes = 0; mi.FAdvance(); ulNodes++)
                {
                    CTreeNode *ptn = const_cast<CTreeNode*>(mi.Pt());
                    (void) ptn->OsPrint(os);
                }

                os << "total number of nodes: " << ulNodes << std::endl;

                return os;
            }

#endif // GPOS_DEBUG

	}; // class CTreeMap
	
}

#endif // !GPOPT_CTreeMap_H

// EOF
