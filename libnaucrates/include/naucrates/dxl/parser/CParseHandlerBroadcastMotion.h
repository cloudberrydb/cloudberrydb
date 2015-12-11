//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerBroadcastMotion.h
//
//	@doc:
//		SAX parse handler class for parsing gather motion operator nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerBroadcastMotion_H
#define GPDXL_CParseHandlerBroadcastMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalBroadcastMotion.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerBroadcastMotion
	//
	//	@doc:
	//		Parse handler for broadcast motion operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerBroadcastMotion : public CParseHandlerPhysicalOp
	{
		private:
						
			// the broadcast motion operator
			CDXLPhysicalBroadcastMotion *m_pdxlop;
			
			// private copy ctor
			CParseHandlerBroadcastMotion(const CParseHandlerBroadcastMotion &);
			
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
			CParseHandlerBroadcastMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerBroadcastMotion_H

// EOF
