//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarOpExpr.h
//
//	@doc:
//		
//		SAX parse handler class for parsing scalar OpExpr.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarOpExpr_H
#define GPDXL_CParseHandlerScalarOpExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarOpExpr.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarOpExpr
	//
	//	@doc:
	//		Parse handler for parsing a scalar op expression
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarOpExpr : public CParseHandlerScalarOp
	{
		private:
	
			ULONG m_ulChildCount;
			// private copy ctor
			CParseHandlerScalarOpExpr(const CParseHandlerScalarOpExpr &);
	
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
			CParseHandlerScalarOpExpr
			(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
			);

	};

}
#endif // GPDXL_CParseHandlerScalarOpExpr_H

//EOF
