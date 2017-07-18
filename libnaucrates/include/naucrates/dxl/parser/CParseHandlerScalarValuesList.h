//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerScalarValuesList.h
//
//	@doc:
//		SAX parse handler class for parsing scalar value list.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarValuesList_H
#define GPDXL_CParseHandlerScalarValuesList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerOp.h"

namespace gpdxl
{
	using namespace gpos;


	XERCES_CPP_NAMESPACE_USE

	// Parse handler for parsing a value list operator
	class CParseHandlerScalarValuesList : public CParseHandlerOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarValuesList(const CParseHandlerScalarValuesList &);

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
			CParseHandlerScalarValuesList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

	};
}

#endif // !GPDXL_CParseHandlerScalarValuesList_H

// EOF
