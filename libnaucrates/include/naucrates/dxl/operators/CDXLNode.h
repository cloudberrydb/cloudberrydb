//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDXLNode.h
//
//	@doc:
//		Basic tree/DAG-based representation for DXL tree nodes
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLNode_H
#define GPDXL_CDXLNode_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLProperties.h"

namespace gpdxl
{
	using namespace gpos;	

	// fwd decl
	class CDXLNode;
	class CDXLOperator;
	class CXMLSerializer;
	class CDXLDirectDispatchInfo;
	
	typedef CDynamicPtrArray<CDXLNode, CleanupRelease> DrgPdxln;

	// arrays of OID
	typedef CDynamicPtrArray<OID, CleanupDelete> DrgPoid;

	typedef CHashMap<ULONG, CDXLNode, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
				CleanupDelete<ULONG>, CleanupRelease<CDXLNode> > HMUlPdxln;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLNode
	//
	//	@doc:
	//		Class for representing nodes in a DXL tree.
	//		Each node specifies an operator at that node, and has an array of children nodes.
	//
	//---------------------------------------------------------------------------
	class CDXLNode : public CRefCount
	{
		private:
		
			// memory pool
			IMemoryPool *m_pmp;
			
			// dxl tree operator class
			CDXLOperator *m_pdxlop;

			// properties of the operator
			CDXLProperties *m_pdxlprop;
			
			// array of children
			DrgPdxln *m_pdrgpdxln;

			// direct dispatch spec
			CDXLDirectDispatchInfo *m_pdxlddinfo;
			
			// private copy ctor
			CDXLNode(const CDXLNode&);
			
		public:
		
			// ctors

			explicit
			CDXLNode(IMemoryPool *pmp);			
			CDXLNode(IMemoryPool *pmp, CDXLOperator *pdxlop);
			CDXLNode(IMemoryPool *pmp, CDXLOperator *pdxlop, CDXLNode *pdxlnChild);
			CDXLNode(IMemoryPool *pmp, CDXLOperator *pdxlop, CDXLNode *pdxlnFst, CDXLNode *pdxlnSnd);
			CDXLNode(IMemoryPool *pmp, CDXLOperator *pdxlop, CDXLNode *pdxlnFst, CDXLNode *pdxlnSnd, CDXLNode *pdxlnThrd);
			CDXLNode(IMemoryPool *pmp, CDXLOperator *pdxlop, DrgPdxln *pdrgpdxln);
			
			// dtor
			virtual
			~CDXLNode();
			
			// shorthand to access children
			inline
			CDXLNode *operator [] 
				(
				ULONG ulPos
				)
				const
			{
				GPOS_ASSERT(NULL != m_pdrgpdxln);
				CDXLNode *pdxln = (*m_pdrgpdxln)[ulPos];
				GPOS_ASSERT(NULL != pdxln);
				return pdxln;
			};
	
			// arity function, returns the number of children this node has
			inline
			ULONG UlArity() const
			{
				return (m_pdrgpdxln == NULL) ? 0 : m_pdrgpdxln->UlLength();
			}
			
			// accessor for operator
			inline
			CDXLOperator *Pdxlop() const
			{
				return m_pdxlop;
			}
			
			// return properties
			CDXLProperties *Pdxlprop() const
			{
				return m_pdxlprop;
			}

			// accessor for children nodes
			const DrgPdxln *PdrgpdxlnChildren() const
			{
				return m_pdrgpdxln;
			}
			
			// accessor to direct dispatch info
			CDXLDirectDispatchInfo *Pdxlddinfo() const
			{
				return m_pdxlddinfo;
			}
			
			// setters
			void AddChild
				(
				CDXLNode *pdxlnChild
				);
			
			void SetOperator
				(
				CDXLOperator *pdxlop
				);

			void SerializeToDXL
				(
				CXMLSerializer *
				)
				const;
			
			// replace a given child of this DXL node with the given node
			void ReplaceChild
				(
				ULONG ulPos,
				CDXLNode *pdxlnChild
				);

			void SerializeChildrenToDXL(CXMLSerializer *pxmlser) const;

			// setter
			void SetProperties(CDXLProperties *pdxlprop);

			// setter for direct dispatch info
			void SetDirectDispatchInfo(CDXLDirectDispatchInfo *pdxlddinfo);
			
			// serialize properties in DXL format
			void SerializePropertiesToDXL(CXMLSerializer *pxmlser) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	}; // class CDXLNode
}


#endif // !GPDXL_CDXLNode_H

// EOF
