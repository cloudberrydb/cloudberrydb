//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarPartOid.h
//
//	@doc:
//		SAX parse handler class for parsing scalar part oid
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarScalarPartOid_H
#define GPDXL_CParseHandlerScalarScalarPartOid_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarPartOid
	//
	//	@doc:
	//		Parse handler class for parsing scalar part oid
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarPartOid : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarPartOid(const CParseHandlerScalarPartOid&);

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
			CParseHandlerScalarPartOid
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarScalarPartOid_H

// EOF
