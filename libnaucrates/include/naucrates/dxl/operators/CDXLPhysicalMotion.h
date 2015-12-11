//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalMotion.h
//
//	@doc:
//		Base class for representing DXL motion operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalMotion_H
#define GPDXL_CDXLPhysicalMotion_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLNode.h"

namespace gpdxl
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalMotion
	//
	//	@doc:
	//		Base class for representing DXL motion operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalMotion : public CDXLPhysical
	{
		private:
			// private copy ctor
			CDXLPhysicalMotion(CDXLPhysicalMotion&);
			
			// serialize the given list of segment ids into a comma-separated string
			CWStringDynamic *PstrSegIds(const DrgPi *pdrgpi) const;

			// serialize input and output segment ids into a comma-separated string
			CWStringDynamic *PstrInputSegIds() const;
			CWStringDynamic *PstrOutputSegIds() const;
			
		protected:
			// list of input segment ids
			DrgPi *m_pdrgpiInputSegIds;
			
			// list of output segment ids
			DrgPi *m_pdrgpiOutputSegIds;

			void SerializeSegmentInfoToDXL(CXMLSerializer *pxmlser) const;

			
		public:
			// ctor/dtor
			explicit
			CDXLPhysicalMotion(IMemoryPool *pmp);

			virtual
			~CDXLPhysicalMotion();
			
			// accessors
			const DrgPi *PdrgpiInputSegIds() const;
			const DrgPi *PdrgpiOutputSegIds() const;
			
			// setters
			void SetInputSegIds(DrgPi *pdrgpi);
			void SetOutputSegIds(DrgPi *pdrgpi);
			void SetSegmentInfo(DrgPi *pdrgpiInputSegIds, DrgPi *pdrgpiOutputSegIds);

			// index of relational child node in the children array
			virtual 
			ULONG UlChildIndex() const = 0;
			
			// conversion function
			static
			CDXLPhysicalMotion *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalMotionGather == pdxlop->Edxlop()
						|| EdxlopPhysicalMotionBroadcast == pdxlop->Edxlop()
						|| EdxlopPhysicalMotionRedistribute == pdxlop->Edxlop()
						|| EdxlopPhysicalMotionRoutedDistribute == pdxlop->Edxlop()
						|| EdxlopPhysicalMotionRandom == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalMotion*>(pdxlop);
			}

	};
}
#endif // !GPDXL_CDXLPhysicalMotion_H

// EOF

