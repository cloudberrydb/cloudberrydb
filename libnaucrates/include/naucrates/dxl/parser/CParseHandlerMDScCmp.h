//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerMDScCmp.h
//
//	@doc:
//		SAX parse handler class for GPDB scalar comparison metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDScCmp_H
#define GPDXL_CParseHandlerMDScCmp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"


#include "naucrates/md/CMDFunctionGPDB.h"


namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDScCmp
	//
	//	@doc:
	//		Parse handler for GPDB scalar comparison metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDScCmp : public CParseHandlerMetadataObject
	{
		private:
			
			// private copy ctor
			CParseHandlerMDScCmp(const CParseHandlerMDScCmp &);
			
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
			CParseHandlerMDScCmp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);			
	};
}

#endif // !GPDXL_CParseHandlerMDScCmp_H

// EOF
