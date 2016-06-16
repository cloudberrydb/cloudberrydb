//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarFuncExpr.h
//
//	@doc:
//		
//		SAX parse handler class for parsing scalar FuncExpr.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarFuncExpr_H
#define GPDXL_CParseHandlerScalarFuncExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarFuncExpr.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarFuncExpr
	//
	//	@doc:
	//		Parse handler for parsing a scalar func expression
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarFuncExpr : public CParseHandlerScalarOp
	{
		private:
	
			BOOL m_fInsideFuncExpr;
	
			// private copy ctor
			CParseHandlerScalarFuncExpr(const CParseHandlerScalarFuncExpr &);
	
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
			CParseHandlerScalarFuncExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};

}
#endif // !GPDXL_CParseHandlerScalarFuncExpr_H

//EOF
