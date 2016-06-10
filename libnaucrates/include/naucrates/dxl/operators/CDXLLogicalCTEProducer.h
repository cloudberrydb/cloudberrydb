//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEProducer.h
//
//	@doc:
//		Class for representing DXL logical CTE producer operators
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalCTEProducer_H
#define GPDXL_CDXLLogicalCTEProducer_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLLogicalCTEProducer
	//
	//	@doc:
	//		Class for representing DXL logical CTE producers
	//
	//---------------------------------------------------------------------------
	class CDXLLogicalCTEProducer : public CDXLLogical
	{
		private:

			// cte id
			ULONG m_ulId;

			// output column ids
			DrgPul *m_pdrgpulColIds;
			
			// private copy ctor
			CDXLLogicalCTEProducer(CDXLLogicalCTEProducer&);

		public:
			// ctor
			CDXLLogicalCTEProducer(IMemoryPool *pmp, ULONG ulId, DrgPul *pdrgpulColIds);
			
			// dtor
			virtual
			~CDXLLogicalCTEProducer();

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
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLLogicalCTEProducer *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopLogicalCTEProducer == pdxlop->Edxlop());
				return dynamic_cast<CDXLLogicalCTEProducer*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLLogicalCTEProducer_H

// EOF
