//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerGatherMotion.h
//
//	@doc:
//		SAX parse handler class for parsing gather motion operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerGatherMotion_H
#define GPDXL_CParseHandlerGatherMotion_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalGatherMotion.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerGatherMotion
	//
	//	@doc:
	//		Parse handler for gather motion operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerGatherMotion : public CParseHandlerPhysicalOp
	{
		private:
			
			// the gather motion operator
			CDXLPhysicalGatherMotion *m_pdxlop;
			
			// private copy ctor
			CParseHandlerGatherMotion(const CParseHandlerGatherMotion &);
			
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
			CParseHandlerGatherMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
	};
}

#endif // !GPDXL_CParseHandlerGatherMotion_H

// EOF
