//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEConsumer.h
//
//	@doc:
//		Class for representing DXL logical CTE Consumer operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalCTEConsumer_H
#define GPDXL_CDXLLogicalCTEConsumer_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalCTEConsumer
	//
	//	@doc:
	//		Class for representing DXL logical CTE Consumers
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalCTEConsumer : public CDXLLogical
	{
		private:

			// cte id
			ULONG m_ulId;

			// output column ids
			DrgPul *m_pdrgpulColIds;
			
			// private copy ctor
			CDXLLogicalCTEConsumer(CDXLLogicalCTEConsumer&);

		public:
			// ctor
			CDXLLogicalCTEConsumer(IMemoryPool *pmp, ULONG ulId, DrgPul *pdrgpulColIds);
			
			// dtor
			virtual
			~CDXLLogicalCTEConsumer();

			// operator type
			virtual
			Edxlopid Edxlop() const;

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// cte identifier
			ULONG UlId() const
			{
				return m_ulId;
			}
			
			DrgPul *PdrgpulColIds() const
			{
				return m_pdrgpulColIds;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			// check if given column is defined by operator
			virtual
			BOOL FDefinesColumn(ULONG ulColId) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
			// conversion function
			static
			CDXLLogicalCTEConsumer *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalCTEConsumer == pdxlop->Edxlop());
				return dynamic_cast<CDXLLogicalCTEConsumer*>(pdxlop);
			}

	};
}
#endif // !GPDXL_CDXLLogicalCTEConsumer_H

// EOF
