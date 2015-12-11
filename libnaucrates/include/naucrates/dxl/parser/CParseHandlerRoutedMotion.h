//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerRoutedMotion.h
//
//	@doc:
//		SAX parse handler class for parsing routed motion operator nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerRoutedMotion_H
#define GPDXL_CParseHandlerRoutedMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalRoutedDistributeMotion.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerRoutedMotion
	//
	//	@doc:
	//		Parse handler for routed motion operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerRoutedMotion : public CParseHandlerPhysicalOp
	{
		private:
			
			// motion operator
			CDXLPhysicalRoutedDistributeMotion *m_pdxlop;
			
			// private copy ctor
			CParseHandlerRoutedMotion(const CParseHandlerRoutedMotion &);
			
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
			CParseHandlerRoutedMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
						
	};
}

#endif // !GPDXL_CParseHandlerRoutedMotion_H

// EOF
