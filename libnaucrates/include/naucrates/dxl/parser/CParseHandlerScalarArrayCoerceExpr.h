//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarArrayCoerceExpr.h
//
//	@doc:
//
//		SAX parse handler class for parsing ArrayCoerceExpr operator.
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarArrayCoerceExpr_H
#define GPDXL_CParseHandlerScalarArrayCoerceExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarArrayCoerceExpr.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarArrayCoerceExpr
	//
	//	@doc:
	//		Parse handler for parsing  parsing ArrayCoerceExpr operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarArrayCoerceExpr : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarArrayCoerceExpr(const CParseHandlerScalarArrayCoerceExpr &);

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
			CParseHandlerScalarArrayCoerceExpr
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

			virtual
			~CParseHandlerScalarArrayCoerceExpr(){};

	};

}
#endif // GPDXL_CParseHandlerScalarArrayCoerceExpr_H

//EOF
