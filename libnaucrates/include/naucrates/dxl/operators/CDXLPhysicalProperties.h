//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalProperties.h
//
//	@doc:
//		Representation of properties of a physical DXL operator
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLPhysicalProperties_H
#define GPDXL_CDXLPhysicalProperties_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLProperties.h"
#include "naucrates/dxl/operators/CDXLOperatorCost.h"

namespace gpdxl
{
	using namespace gpos;	

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalProperties
	//
	//	@doc:
	//		Container for the properties of a physical operator node, such as
	//		cost and output columns
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalProperties : public CDXLProperties
	{
		private:
			// cost estimate
			CDXLOperatorCost *m_pdxlopcost;

			// private copy ctor
			CDXLPhysicalProperties(const CDXLPhysicalProperties&);
			
		public:

			// ctor
			explicit
			CDXLPhysicalProperties(CDXLOperatorCost *pdxlopcost);
			
			// dtor
			virtual
			~CDXLPhysicalProperties();

			// serialize properties in DXL format
			void SerializePropertiesToDXL(CXMLSerializer *pxmlser) const;

			// accessors
			// the cost estimates for the operator node
			CDXLOperatorCost *Pdxlopcost() const;

			virtual
			Edxlprop Edxlproptype() const
			{
				return EdxlpropPhysical;
			}

			// conversion function
			static
			CDXLPhysicalProperties *PdxlpropConvert
				(
				CDXLProperties *pdxlprop
				)
			{
				GPOS_ASSERT(NULL != pdxlprop);
				GPOS_ASSERT(EdxlpropPhysical == pdxlprop->Edxlproptype());
				return dynamic_cast<CDXLPhysicalProperties *>(pdxlprop);
			}
	};

}

#endif // !GPDXL_CDXLPhysicalProperties_H

// EOF
