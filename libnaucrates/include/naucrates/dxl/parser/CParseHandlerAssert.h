//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerAssert.h
//
//	@doc:
//		SAX parse handler class for parsing assert operator nodes
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerAssert_H
#define GPDXL_CParseHandlerAssert_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerAssert
	//
	//	@doc:
	//		Parse handler for DXL physical assert 
	//
	//---------------------------------------------------------------------------
	class CParseHandlerAssert : public CParseHandlerPhysicalOp
	{
		private:
			
			// physical assert operator
			CDXLPhysicalAssert *m_pdxlop;
			
			// private copy ctor
			CParseHandlerAssert(const CParseHandlerAssert&);
			
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
			CParseHandlerAssert
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerAssert_H

// EOF
