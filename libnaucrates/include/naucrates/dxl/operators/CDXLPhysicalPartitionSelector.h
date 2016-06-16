//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalPartitionSelector.h
//
//	@doc:
//		Class for representing DXL partition selector
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalPartitionSelector_H
#define GPDXL_CDXLPhysicalPartitionSelector_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
	// indices of partition selector elements in the children array
	enum Edxlps
	{
		EdxlpsIndexProjList = 0,
		EdxlpsIndexEqFilters,
		EdxlpsIndexFilters,
		EdxlpsIndexResidualFilter,
		EdxlpsIndexPropExpr,
		EdxlpsIndexPrintableFilter,
		EdxlpsIndexChild,
		EdxlpsIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalPartitionSelector
	//
	//	@doc:
	//		Class for representing DXL partition selector
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalPartitionSelector : public CDXLPhysical
	{
		private:

			// table id
			IMDId *m_pmdidRel;

			// number of partitioning levels
			ULONG m_ulLevels;
			
			// scan id
			ULONG m_ulScanId;

			// private copy ctor
			CDXLPhysicalPartitionSelector(CDXLPhysicalPartitionSelector&);

		public:
			// ctor
			CDXLPhysicalPartitionSelector(IMemoryPool *pmp, IMDId *pmdidRel, ULONG ulLevels, ULONG ulScanId);
			
			// dtor
			virtual
			~CDXLPhysicalPartitionSelector();
			
			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;
			
			// table id
			IMDId *PmdidRel() const
			{
				return m_pmdidRel;
			}

			// number of partitioning levels
			ULONG UlLevels() const
			{
				return m_ulLevels;
			}

			// scan id
			ULONG UlScanId() const
			{
				return m_ulScanId;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			virtual
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLPhysicalPartitionSelector *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalPartitionSelector == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalPartitionSelector*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLPhysicalPartitionSelector_H

// EOF

