//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//	Portions Copyright (c) 2023, HashData Technology Limited.
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

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"

#include "gpopt/translate/CMappingElementColIdParamId.h"


// fwd decl
struct TargetEntry;

namespace gpdxl
{
using namespace gpos;

// hash maps mapping ULONG -> TargetEntry
using ULongToTargetEntryMap =
	CHashMap<ULONG, TargetEntry, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
			 CleanupDelete<ULONG>, CleanupNULL>;

using ULongToTargetEntryMapIter =
	CHashMapIter<ULONG, TargetEntry, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
				 CleanupDelete<ULONG>, CleanupNULL>;

// hash maps mapping ULONG -> CMappingElementColIdParamId
using ULongToColParamMap =
	CHashMap<ULONG, CMappingElementColIdParamId, gpos::HashValue<ULONG>,
			 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
			 CleanupRelease<CMappingElementColIdParamId>>;

using ULongToColParamMapIter =
	CHashMapIter<ULONG, CMappingElementColIdParamId, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
				 CleanupRelease<CMappingElementColIdParamId>>;


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
	CMemoryPool *m_mp;

	// mappings ColId->TargetEntry used for intermediate DXL nodes
	ULongToTargetEntryMap *m_colid_to_target_entry_map;

	// mappings ColId->ParamId used for outer refs in subplans
	ULongToColParamMap *m_colid_to_paramid_map;

	// is the node for which this context is built a child of an aggregate node
	// This is used to assign 0 instead of OUTER for the varno value of columns
	// in an Agg node, as expected in GPDB
	// TODO: antovl - Jan 26, 2011; remove this when Agg node in GPDB is fixed
	// to use OUTER instead of 0 for Var::varno in Agg target lists (MPP-12034)
	BOOL m_is_child_agg_node;

	// copy the params hashmap
	void CopyParamHashmap(ULongToColParamMap *original);
	void CopyTargetEntryHashmap(ULongToTargetEntryMap *original);

public:
	CDXLTranslateContext(const CDXLTranslateContext &) = delete;

	// ctor/dtor
	CDXLTranslateContext(CMemoryPool *mp, BOOL is_child_agg_node);

	CDXLTranslateContext(CMemoryPool *mp, BOOL is_child_agg_node,
						 ULongToColParamMap *original);

	~CDXLTranslateContext();

	// is parent an aggregate node
	BOOL IsParentAggNode() const;

	// return the params hashmap
	ULongToColParamMap *
	GetColIdToParamIdMap()
	{
		return m_colid_to_paramid_map;
	}

	ULongToTargetEntryMap*
	GetColIdToTargetEntryMap() const
	{
		return m_colid_to_target_entry_map;
	}

	// return the target entry corresponding to the given ColId
	const TargetEntry *GetTargetEntry(ULONG colid) const;

	// return the param id corresponding to the given ColId
	const CMappingElementColIdParamId *GetParamIdMappingElement(
		ULONG colid) const;

	// store the mapping of the given column id and target entry
	void InsertMapping(ULONG colid, TargetEntry *target_entry);

	// store the mapping of the given column id and param id
	BOOL FInsertParamMapping(ULONG colid,
							 CMappingElementColIdParamId *pmecolidparamid);
	// merge another context
	void MergeTcxt(CDXLTranslateContext *tcxt);
};


// array of dxl translation context
using CDXLTranslationContextArray =
	CDynamicPtrArray<const CDXLTranslateContext, CleanupNULL>;
}  // namespace gpdxl

#endif	// !GPDXL_CDXLTranslateContext_H

// EOF
