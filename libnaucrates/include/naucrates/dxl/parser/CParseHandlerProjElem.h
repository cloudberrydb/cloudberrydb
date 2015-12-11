//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerProjElem.h
//
//	@doc:
//		SAX parse handler class for parsing projection elements.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerProjElem_H
#define GPDXL_CParseHandlerProjElem_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarProjElem.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerProjElem
	//
	//	@doc:
	//		Parse handler for projection elements
	//
	//---------------------------------------------------------------------------
	class CParseHandlerProjElem : public CParseHandlerScalarOp
	{
		private:
					
			// project elem operator
			CDXLScalarProjElem *m_pdxlop;
						
			// private copy ctor
			CParseHandlerProjElem(const CParseHandlerProjElem&); 
			
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
			CParseHandlerProjElem
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			
	};
}

#endif // !GPDXL_CParseHandlerProjElem_H

// EOF
