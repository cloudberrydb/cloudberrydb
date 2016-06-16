//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalCTEProducer.h
//
//	@doc:
//		Class for representing DXL physical CTE producer operators
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLPhysicalCTEProducer_H
#define GPDXL_CDXLPhysicalCTEProducer_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalCTEProducer
	//
	//	@doc:
	//		Class for representing DXL physical CTE producers
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalCTEProducer : public CDXLPhysical
	{
		private:

			// cte id
			ULONG m_ulId;

			// output column ids
			DrgPul *m_pdrgpulColIds;

			// private copy ctor
			CDXLPhysicalCTEProducer(CDXLPhysicalCTEProducer&);

		public:
			// ctor
			CDXLPhysicalCTEProducer(IMemoryPool *pmp, ULONG ulId, DrgPul *pdrgpulColIds);

			// dtor
			virtual
			~CDXLPhysicalCTEProducer();

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
			CDXLPhysicalCTEProducer *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalCTEProducer == pdxlop->Edxlop());
				return dynamic_cast<CDXLPhysicalCTEProducer*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLPhysicalCTEProducer_H

// EOF
