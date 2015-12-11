//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalCTEConsumer.h
//
//	@doc:
//		Class for representing DXL physical CTE Consumer operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLPhysicalCTEConsumer_H
#define GPDXL_CDXLPhysicalCTEConsumer_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalCTEConsumer
	//
	//	@doc:
	//		Class for representing DXL physical CTE Consumers
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalCTEConsumer : public CDXLPhysical
	{
		private:

			// cte id
			ULONG m_ulId;

			// output column ids
			DrgPul *m_pdrgpulColIds;

			// private copy ctor
			CDXLPhysicalCTEConsumer(CDXLPhysicalCTEConsumer&);

		public:
			// ctor
			CDXLPhysicalCTEConsumer(IMemoryPool *pmp, ULONG ulId, DrgPul *pdrgpulColIds);

			// dtor
			virtual
			~CDXLPhysicalCTEConsumer();

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


#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLPhysicalCTEConsumer *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalCTEConsumer == pdxlop->Edxlop());
				return dynamic_cast<CDXLPhysicalCTEConsumer*>(pdxlop);
			}

	};
}
#endif // !GPDXL_CDXLPhysicalCTEConsumer_H

// EOF
