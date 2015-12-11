//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerMDCast.h
//
//	@doc:
//		SAX parse handler class for GPDB cast metadata
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDCast_H
#define GPDXL_CParseHandlerMDCast_H

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
	//		CParseHandlerMDCast
	//
	//	@doc:
	//		Parse handler for GPDB cast function metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDCast : public CParseHandlerMetadataObject
	{
		private:
			
			// private copy ctor
			CParseHandlerMDCast(const CParseHandlerMDCast &);
			
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
			CParseHandlerMDCast
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);			
	};
}

#endif // !GPDXL_CParseHandlerMDCast_H

// EOF
