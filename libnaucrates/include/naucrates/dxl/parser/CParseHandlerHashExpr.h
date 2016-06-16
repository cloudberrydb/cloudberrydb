//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerHashExpr.h
//
//	@doc:
//		SAX parse handler class for parsing a hash expressions 
//		in the hash expr list of a redistribute motion node.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerHashExpr_H
#define GPDXL_CParseHandlerHashExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalarHashExpr.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerHashExpr
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of hash expressions
	//		in the hash expr list of a redistribute motion node.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerHashExpr : public CParseHandlerScalarOp
	{
		private:
					
			// hash expr operator
			CDXLScalarHashExpr *m_pdxlop;
						
			// private copy ctor
			CParseHandlerHashExpr(const CParseHandlerHashExpr&); 
		
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
			CParseHandlerHashExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerHashExpr_H

// EOF
