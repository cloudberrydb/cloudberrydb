//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLTranslateContext.h
//
//	@doc:
//		Class providing access to translation context, such as mappings between
//		table names, operator names, etc. and oids
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLTranslateContext_H
#define GPDXL_CDXLTranslateContext_H

#include "gpopt/translate/CMappingElementColIdParamId.h"

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"


// fwd decl
struct TargetEntry;

namespace gpdxl
{

	using namespace gpos;

	// hash maps mapping ULONG -> TargetEntry
	typedef CHashMap<ULONG, TargetEntry, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
		CleanupDelete<ULONG>, CleanupNULL > HMUlTe;

	// hash maps mapping ULONG -> CMappingElementColIdParamId
	typedef CHashMap<ULONG, CMappingElementColIdParamId, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
		CleanupDelete<ULONG>, CleanupRelease<CMappingElementColIdParamId> > HMColParam;

	typedef CHashMapIter<ULONG, CMappingElementColIdParamId, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<CMappingElementColIdParamId> > HMColParamIter;


	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLTranslateContext
	//
	//	@doc:
	//		Class providing access to translation context, such as mappings between
	//		ColIds and target entries
	//
	//---------------------------------------------------------------------------
	class CDXLTranslateContext
	{

		private:
			IMemoryPool *m_pmp;

			// private copy ctor
			CDXLTranslateContext(const CDXLTranslateContext&);

			// mappings ColId->TargetEntry used for intermediate DXL nodes
			HMUlTe *m_phmulte;

			// mappings ColId->ParamId used for outer refs in subplans
			HMColParam *m_phmcolparam;

			// is the node for which this context is built a child of an aggregate node
			// This is used to assign 0 instead of OUTER for the varno value of columns
			// in an Agg node, as expected in GPDB
			// TODO: antovl - Jan 26, 2011; remove this when Agg node in GPDB is fixed
			// to use OUTER instead of 0 for Var::varno in Agg target lists (MPP-12034)
			BOOL m_fChildAggNode;

			// copy the params hashmap
			void CopyParamHashmap(HMColParam *phmOriginal);

		public:
			// ctor/dtor
			CDXLTranslateContext(IMemoryPool *pmp, BOOL fChildAggNode);

			CDXLTranslateContext(IMemoryPool *pmp, BOOL fChildAggNode, HMColParam *phmOriginal);

			~CDXLTranslateContext();

			// is parent an aggregate node
			BOOL FParentAggNode() const;

			// return the params hashmap
			HMColParam *PhmColParam()
			{
				return m_phmcolparam;
			}

			// return the target entry corresponding to the given ColId
			const TargetEntry *Pte(ULONG ulColId) const;

			// return the param id corresponding to the given ColId
			const CMappingElementColIdParamId *Pmecolidparamid(ULONG ulColId) const;

			// store the mapping of the given column id and target entry
			void InsertMapping(ULONG ulColId, TargetEntry *pte);

			// store the mapping of the given column id and param id
			BOOL FInsertParamMapping(ULONG ulColId, CMappingElementColIdParamId *pmecolidparamid);
	};


	// array of dxl translation context
	typedef CDynamicPtrArray<const CDXLTranslateContext, CleanupNULL> DrgPdxltrctx;
}

#endif // !GPDXL_CDXLTranslateContext_H

// EOF
