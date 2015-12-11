//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerDefaultValueExpr.h
//
//	@doc:
//		SAX parse handler class for parsing the default column value expression in
//		a column's metadata info.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerDefaultValueExpr_H
#define GPDXL_CParseHandlerDefaultValueExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerDefaultValueExpr
	//
	//	@doc:
	//		Parse handler for parsing a default value expression in column metadata info
	//
	//---------------------------------------------------------------------------
	class CParseHandlerDefaultValueExpr : public CParseHandlerScalarOp
	{
		private:

			// has an opening tag for a default value been seen already
			BOOL m_fDefaultValueStarted;
			
			// private copy ctor
			CParseHandlerDefaultValueExpr(const CParseHandlerDefaultValueExpr &);
			
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
			CParseHandlerDefaultValueExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerDefaultValueExpr_H

// EOF
