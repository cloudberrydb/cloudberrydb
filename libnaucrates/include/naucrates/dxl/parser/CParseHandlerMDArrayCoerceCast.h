//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerMDArrayCoerceCast.h
//
//	@doc:
//		SAX parse handler class for GPDB array coerce cast metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDArrayCoerceCast_H
#define GPDXL_CParseHandlerMDArrayCoerceCast_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"


#include "naucrates/md/CMDFunctionGPDB.h"


namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE

	// parse handler for GPDB array coerce cast function metadata
	class CParseHandlerMDArrayCoerceCast : public CParseHandlerMetadataObject
	{
		private:

			// private copy ctor
			CParseHandlerMDArrayCoerceCast(const CParseHandlerMDArrayCoerceCast &);

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
			CParseHandlerMDArrayCoerceCast
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerMDArrayCoerceCast_H

// EOF
