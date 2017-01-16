//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarCoerceViaIO.h
//
//	@doc:
//
//		SAX parse handler class for parsing CoerceViaIO operator.
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarCoerceViaIO_H
#define GPDXL_CParseHandlerScalarCoerceViaIO_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarCoerceViaIO.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarCoerceViaIO
	//
	//	@doc:
	//		Parse handler for parsing CoerceViaIO operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarCoerceViaIO : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarCoerceViaIO(const CParseHandlerScalarCoerceViaIO &);

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
			CParseHandlerScalarCoerceViaIO
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

			virtual
			~CParseHandlerScalarCoerceViaIO(){};

	};

}
#endif // GPDXL_CParseHandlerScalarCoerceViaIO_H

//EOF
