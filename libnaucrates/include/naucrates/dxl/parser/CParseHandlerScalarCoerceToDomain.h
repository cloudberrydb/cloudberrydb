//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarCoerceToDomain.h
//
//	@doc:
//
//		SAX parse handler class for parsing CoerceToDomain operator.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarCoerceToDomain_H
#define GPDXL_CParseHandlerScalarCoerceToDomain_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarCoerceToDomain.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarCoerceToDomain
	//
	//	@doc:
	//		Parse handler for parsing CoerceToDomain operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarCoerceToDomain : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarCoerceToDomain(const CParseHandlerScalarCoerceToDomain &);

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
			CParseHandlerScalarCoerceToDomain
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

			virtual
			~CParseHandlerScalarCoerceToDomain(){};

	};

}
#endif // GPDXL_CParseHandlerScalarCoerceToDomain_H

//EOF
