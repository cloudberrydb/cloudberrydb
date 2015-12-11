//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerResult.h
//
//	@doc:
//		SAX parse handler class for parsing result operator nodes.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerResult_H
#define GPDXL_CParseHandlerResult_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalResult.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerResult
	//
	//	@doc:
	//		Parse handler for parsing a result operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerResult : public CParseHandlerPhysicalOp
	{
		private:

			// the result operator
			CDXLPhysicalResult *m_pdxlop;

			// private copy ctor
			CParseHandlerResult(const CParseHandlerResult &);

			// set up initial handlers
			void SetupInitialHandlers();

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
			CParseHandlerResult
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerResult_H

// EOF
