//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarDMLAction.h
//
//	@doc:
//		SAX parse handler class for parsing scalar DML action expression
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarDMLAction_H
#define GPDXL_CParseHandlerScalarDMLAction_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarDMLAction
	//
	//	@doc:
	//		Parse handler for parsing a scalar DML action expression
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarDMLAction : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarDMLAction(const CParseHandlerScalarDMLAction &);
			
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
			CParseHandlerScalarDMLAction
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarDMLAction_H

// EOF
