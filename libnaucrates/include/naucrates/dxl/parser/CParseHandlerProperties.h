//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerProperties.h
//
//	@doc:
//		SAX parse handler class for parsing properties of physical operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerProperties_H
#define GPDXL_CParseHandlerProperties_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/md/CDXLStatsDerivedRelation.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerProperties
	//
	//	@doc:
	//		Parse handler for parsing the properties of a physical operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerProperties : public CParseHandlerBase 
	{
		private:
			// physical properties container
			CDXLPhysicalProperties *m_pdxlprop;
			
			// statistics of the physical plan
			CDXLStatsDerivedRelation *m_pdxlstatsderrel;

			// private ctor
			CParseHandlerProperties(const CParseHandlerProperties &);
			
			// process the start of an element
			void StartElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
				);
				
			// process the end of an element
			void EndElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
				);
			
		public:
			// ctor
			CParseHandlerProperties
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual
			~CParseHandlerProperties();
					
			// returns the constructed properties container
			CDXLPhysicalProperties *Pdxlprop() const;

			// return the derived relation statistics
			CDXLStatsDerivedRelation *Pdxlstatsderrel() const
			{
				return m_pdxlstatsderrel;
			}
	};
}

#endif // !GPDXL_CParseHandlerProperties_H

// EOF
