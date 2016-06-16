//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalGroupBy.h
//
//	@doc:
//		Class for representing DXL logical group by operators
//		
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalGroupBy_H
#define GPDXL_CDXLLogicalGroupBy_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"
#include "naucrates/dxl/operators/CDXLNode.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalGroupBy
	//
	//	@doc:
	//		Class for representing DXL logical group by operators
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalGroupBy : public CDXLLogical
	{
		private:

			// grouping column ids
			DrgPul *m_pdrgpulGrpColId;

			// private copy ctor
			CDXLLogicalGroupBy(CDXLLogicalGroupBy&);

			// serialize output grouping columns indices in DXL
			void SerializeGrpColsToDXL(CXMLSerializer *) const;

		public:
			// ctors
			explicit
			CDXLLogicalGroupBy(IMemoryPool *pmp);
			CDXLLogicalGroupBy(IMemoryPool *pmp, DrgPul *pdrgpulGrpColIds);

			// dtor
			virtual
			~CDXLLogicalGroupBy();

			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			const DrgPul *PdrgpulGroupingCols() const;

			// set grouping column indices
			void SetGroupingColumns(DrgPul *);

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// conversion function
			static
			CDXLLogicalGroupBy *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalGrpBy == pdxlop->Edxlop());

				return dynamic_cast<CDXLLogicalGroupBy*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

	};
}
#endif // !GPDXL_CDXLLogicalGroupBy_H

// EOF
