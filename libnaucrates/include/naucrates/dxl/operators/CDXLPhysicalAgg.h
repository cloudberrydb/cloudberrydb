//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalAgg.h
//
//	@doc:
//		Class for representing DXL aggregate operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalAgg_H
#define GPDXL_CDXLPhysicalAgg_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	// indices of group by elements in the children array
	enum Edxlagg
	{
		EdxlaggIndexProjList = 0,
		EdxlaggIndexFilter,
		EdxlaggIndexChild,
		EdxlaggIndexSentinel
	};
	
	enum EdxlAggStrategy
	{
		EdxlaggstrategyPlain,
		EdxlaggstrategySorted,
		EdxlaggstrategyHashed,
		EdxlaggstrategySentinel
	};
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalAgg
	//
	//	@doc:
	//		Class for representing DXL aggregate operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalAgg : public CDXLPhysical
	{
		private:
			
			// private copy ctor
			CDXLPhysicalAgg(const CDXLPhysicalAgg&);
			
			// grouping column ids
			DrgPul *m_pdrgpulGroupingCols;
			
			EdxlAggStrategy m_edxlaggstr;
			
			// is it safe to stream the local hash aggregate
			BOOL m_fStreamSafe;
			
			// serialize output grouping columns indices in DXL
			void SerializeGroupingColsToDXL(CXMLSerializer *pxmlser) const;
			
		public:
			// ctor
			CDXLPhysicalAgg
				(
				IMemoryPool *pmp,
				EdxlAggStrategy edxlaggstr,
				BOOL fStreamSafe
				);
			
			// dtor
			virtual
			~CDXLPhysicalAgg();

			// accessors
			Edxlopid Edxlop() const;
			EdxlAggStrategy Edxlaggstr() const;
			
			const CWStringConst *PstrOpName() const;
			const CWStringConst *PstrAggStrategy() const;
			const CWStringConst *PstrAggLevel() const;
			const DrgPul *PdrgpulGroupingCols() const;
			
			// set grouping column indices
			void SetGroupingCols(DrgPul *);
			
			// is aggregate a hash aggregate that it safe to stream
			BOOL FStreamSafe() const
			{
				return (EdxlaggstrategyHashed == m_edxlaggstr) && m_fStreamSafe;
			}
			
			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLPhysicalAgg *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalAgg == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalAgg*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalAgg_H

// EOF

