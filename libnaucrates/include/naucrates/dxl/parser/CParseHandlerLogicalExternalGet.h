//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerLogicalExternalGet.h
//
//	@doc:
//		SAX parse handler class for parsing logical external get operator node
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerLogicalExternalGet_H
#define GPDXL_CParseHandlerLogicalExternalGet_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalGet.h"

namespace gpdxl
{
	using namespace gpos;


	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalExternalGet
	//
	//	@doc:
	//		Parse handler for parsing a logical external get operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalExternalGet : public CParseHandlerLogicalGet
	{
		private:

			// private copy ctor
			CParseHandlerLogicalExternalGet(const CParseHandlerLogicalExternalGet &);

			// process the start of an element
			virtual
			void StartElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
				);

			// process the end of an element
			virtual
			void EndElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
				);

		public:
			// ctor
			CParseHandlerLogicalExternalGet
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

	};
}

#endif // !GPDXL_CParseHandlerLogicalExternalGet_H

// EOF
