//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTreeMap.inl
//
//	@doc:
//		Implementation of template functions for tree map
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CTreeMap_INL
#define GPOPT_CTreeMap_INL


namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::CTreeNode
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::CTreeNode
		(
		IMemoryPool *pmp,
		ULONG ul,
		const T *pt
		)
		:
		m_pmp(pmp),
		m_ul(ul),
		m_pt(pt),
		m_pdrgdrgptn(NULL),
		m_ullCount(ULLONG_MAX),
		m_ulIncoming(0),
		m_ens(EnsUncounted)
	{
		m_pdrgdrgptn = GPOS_NEW(pmp) DrgDrgPtn(pmp);
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::~CTreeNode
	//
	//	@doc:
	//		dtor
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::~CTreeNode()
	{
		m_pdrgdrgptn->Release();
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::Add
	//
	//	@doc:
	//		Add child candidate
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	void
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::Add
		(
		ULONG ulPos,
		CTreeNode *ptn
		)
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::FCounted
	//
	//	@doc:
	//		Check if cached count is valid
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	BOOL
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::FCounted() const
	{
		return (EnsCounted == m_ens);
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::UllCount
	//
	//	@doc:
	//		Recursively count trees using dynamic programming unless count
	//		is already cached
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	ULLONG
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::UllCount()
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::UllCount
	//
	//	@doc:
	//		Count on a per-child basis: for a given child add up all alternatives
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	ULLONG
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::UllCount
		(
		ULONG ulChild
		)
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
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::PrUnrank
	//
	//	@doc:
	//		Locally unrank a specific child
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	R *
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::PrUnrank
		(
		IMemoryPool *pmp,
		PrFn prfn,
		U *pU,
		ULONG ulChild,
		ULLONG ullRank
		)
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::PrUnrank
	//
	//	@doc:
	//		Recursively rehydrate tree
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	R *
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::PrUnrank
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeMap
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeMap
		(
		IMemoryPool *pmp,
		PrFn prfn
		)
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::~CTreeMap
	//
	//	@doc:
	//		dtor
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	CTreeMap<T, R, U, pfnHash, pfnEq>::~CTreeMap()
	{
		m_ptmap->Release();
		m_plinkmap->Release();
		
		GPOS_DELETE(m_ptnRoot);
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::Ptn
	//
	//	@doc:
	//		Find tree node corresponding to source node; create as needed
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	typename CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode*
	CTreeMap<T, R, U, pfnHash, pfnEq>::Ptn
		(
		const T *pt
		)
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::Insert
	//
	//	@doc:
	//		Insertion of edge
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
			ULONG (*pfnHash)(const T*),
			BOOL (*pfnEq)(const T*, const T*)>
	void
	CTreeMap<T, R, U, pfnHash, pfnEq>::Insert
		(
		const T *ptParent,
		ULONG ulPos,
		const T *ptChild
		)
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

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::InsertRoot
	//
	//	@doc:
	//		Insertion root node
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
				ULONG (*pfnHash)(const T*),
				BOOL (*pfnEq)(const T*, const T*)>
	void
	CTreeMap<T, R, U, pfnHash, pfnEq>::InsertRoot
		(
		const T *pt
		)
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(NULL != m_ptnRoot);
		
		// add logical root as 0-th child to global root
		m_ptnRoot->Add(0 /*ulPos*/, Ptn(pt));
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::UllCount
	//
	//	@doc:
	//		Count all possible trees
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
					ULONG (*pfnHash)(const T*),
					BOOL (*pfnEq)(const T*, const T*)>
	ULLONG
	CTreeMap<T, R, U, pfnHash, pfnEq>::UllCount()
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::UlRank
	//
	//	@doc:
	//		Unrank a tree
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
					ULONG (*pfnHash)(const T*),
					BOOL (*pfnEq)(const T*, const T*)>
	R *
	CTreeMap<T, R, U, pfnHash, pfnEq>::PrUnrank
		(
		IMemoryPool *pmp,
		U *pU,
		ULLONG ullRank
		)
		const
	{
		return m_ptnRoot->PrUnrank(pmp, m_prfn, pU, ullRank);
	}


#ifdef GPOS_DEBUG

	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::UllCount
	//
	//	@doc:
	//		Count trees for given element
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
					ULONG (*pfnHash)(const T*),
					BOOL (*pfnEq)(const T*, const T*)>
	ULLONG
	CTreeMap<T, R, U, pfnHash, pfnEq>::UllCount
		(
		const T *pt
		)
	{
		CTreeNode *ptn = m_ptmap->PtLookup(pt); 			
		GPOS_ASSERT(NULL != ptn);
		
		return ptn->UllCount();
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::CTreeNode::OsPrint
	//
	//	@doc:
	//		Debug print of individual node
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
					ULONG (*pfnHash)(const T*),
					BOOL (*pfnEq)(const T*, const T*)>
	IOstream &
	CTreeMap<T, R, U, pfnHash, pfnEq>::CTreeNode::OsPrint
		(
		IOstream &os
		)
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
	

	//---------------------------------------------------------------------------
	//	@function:
	//		CTreeMap::OsPrint
	//
	//	@doc:
	//		Debug print of entire map
	//
	//---------------------------------------------------------------------------
	template <class T, class R, class U,
					ULONG (*pfnHash)(const T*),
					BOOL (*pfnEq)(const T*, const T*)>
	IOstream &
	CTreeMap<T, R, U, pfnHash, pfnEq>::OsPrint
		(
		IOstream &os
		)
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
	
}

#endif // !GPOPT_CTreeMap_INL

// EOF
