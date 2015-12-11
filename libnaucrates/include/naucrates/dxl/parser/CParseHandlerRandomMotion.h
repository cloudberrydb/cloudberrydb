//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerRandomMotion.h
//
//	@doc:
//		SAX parse handler class for parsing random motion operator nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerRandomMotion_H
#define GPDXL_CParseHandlerRandomMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalRandomMotion.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerRandomMotion
	//
	//	@doc:
	//		Parse handler for routed motion operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerRandomMotion : public CParseHandlerPhysicalOp
	{
		private:
			
			// motion operator
			CDXLPhysicalRandomMotion *m_pdxlop;
			
			// private copy ctor
			CParseHandlerRandomMotion(const CParseHandlerRandomMotion &);
			
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
			CParseHandlerRandomMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
						
	};
}

#endif // !GPDXL_CParseHandlerRandomMotion_H

// EOF
