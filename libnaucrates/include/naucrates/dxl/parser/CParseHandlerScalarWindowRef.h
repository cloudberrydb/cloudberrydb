//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarWindowRef.h
//
//	@doc:
//		
//		SAX parse handler class for parsing scalar WindowRef
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarWindowRef_H
#define GPDXL_CParseHandlerScalarWindowRef_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarWindowRef.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarWindowRef
	//
	//	@doc:
	//		Parse handler for parsing a scalar WindowRef
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarWindowRef : public CParseHandlerScalarOp
	{
		private:
			// private copy ctor
			CParseHandlerScalarWindowRef(const CParseHandlerScalarWindowRef &);

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
			CParseHandlerScalarWindowRef
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};

}
#endif // !GPDXL_CParseHandlerScalarWindowRef_H

//EOF
