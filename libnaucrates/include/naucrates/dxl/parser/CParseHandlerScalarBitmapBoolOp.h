//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarBitmapBoolOp.h
//
//	@doc:
//		
//		SAX parse handler class for parsing scalar bitmap bool expressions
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarBitmapBoolOp_H
#define GPDXL_CParseHandlerScalarBitmapBoolOp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarBoolExpr.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarBitmapBoolOp
	//
	//	@doc:
	//		Parse handler for parsing a scalar bitmap bool expression
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarBitmapBoolOp : public CParseHandlerScalarOp
	{
		private:
		
			// private copy ctor
			CParseHandlerScalarBitmapBoolOp(const CParseHandlerScalarBitmapBoolOp &);
	
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
	
			// parse the bool type from the Xerces xml string
			static
			EdxlBoolExprType EdxlBoolType(const XMLCh *xmlszBoolType);

		public:
			// ctor
			CParseHandlerScalarBitmapBoolOp
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

	};

}
#endif // GPDXL_CParseHandlerScalarBitmapBoolOp_H

//EOF
