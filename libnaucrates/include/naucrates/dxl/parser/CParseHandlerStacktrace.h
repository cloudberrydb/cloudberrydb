//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStacktrace.h
//
//	@doc:
//		Pass-through parse handler class for stack traces
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerStacktrace_H
#define GPDXL_CParseHandlerStacktrace_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerStacktrace
	//
	//	@doc:
	//		Pass-through parse handler class for stack traces
	//
	//---------------------------------------------------------------------------
	class CParseHandlerStacktrace : public CParseHandlerBase
	{
		private:
											
			// private copy ctor
			CParseHandlerStacktrace(const CParseHandlerStacktrace&);
		
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
			// ctor/dtor
			CParseHandlerStacktrace
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerStacktrace_H

// EOF
