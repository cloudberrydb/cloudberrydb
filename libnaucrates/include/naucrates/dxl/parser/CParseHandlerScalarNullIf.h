//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarNullIf.h
//
//	@doc:
//		SAX parse handler class for parsing scalar NullIf operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarNullIf_H
#define GPDXL_CParseHandlerScalarNullIf_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarNullIf.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarNullIf
	//
	//	@doc:
	//		Parse handler for parsing scallar NullIf operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarNullIf : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarNullIf(const CParseHandlerScalarNullIf &);

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
			CParseHandlerScalarNullIf
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarNullIf_H

// EOF
