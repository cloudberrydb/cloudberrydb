//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLProperties.h
//
//	@doc:
//		Representation of properties of a DXL operator node such as stats
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLProperties_H
#define GPDXL_CDXLProperties_H

#include "gpos/base.h"
#include "naucrates/md/CDXLStatsDerivedRelation.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	enum Edxlprop
	{
		EdxlpropLogical,
		EdxlpropPhysical,
		EdxlpropSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLProperties
	//
	//	@doc:
	//		Container for the properties of an operator node, such as stats
	//
	//---------------------------------------------------------------------------
	class CDXLProperties : public CRefCount
	{
		private:

			// derived statistics
			CDXLStatsDerivedRelation *m_pdxlstatsderrel;

			// private copy ctor
			CDXLProperties(const CDXLProperties&);

		protected:

			// serialize statistics in DXL format
			void SerializeStatsToDXL(CXMLSerializer *pxmlser) const;

		public:

			// ctor
			explicit
			CDXLProperties();

			//dtor
			virtual
			~CDXLProperties();

			// setter
			virtual
			void SetStats(CDXLStatsDerivedRelation *pdxlstatsderrel);

			// statistical information
			virtual
			const CDXLStatsDerivedRelation *Pdxlstatsderrel() const;

			virtual
			Edxlprop Edxlproptype() const
			{
				return EdxlpropLogical;
			}

			// serialize properties in DXL format
			virtual
			void SerializePropertiesToDXL(CXMLSerializer *pxmlser) const;
	};

}

#endif // !GPDXL_CDXLProperties_H

// EOF
