//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerAppend.h
//
//	@doc:
//		SAX parse handler class for parsing Append operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerAppend_H
#define GPDXL_CParseHandlerAppend_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/operators/CDXLPhysicalAppend.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerAppend
	//
	//	@doc:
	//		Parse handler for Append operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerAppend : public CParseHandlerPhysicalOp
	{
		private:

				CDXLPhysicalAppend *m_pdxlop;

				// private copy ctor
				CParseHandlerAppend(const CParseHandlerAppend &);

				// set up initial handlers
				void SetupInitialHandlers(const Attributes& attrs);
				
				// process the start of an element
				void StartElement
					(
						const XMLCh* const xmlszUri, 		// URI of element's namespace
	 					const XMLCh* const xmlszLocalname,	// local part of element's name
						const XMLCh* const xmlszQname,		// element's qname
						const Attributes& attrs				// element's attributes
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
				CParseHandlerAppend
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);
	};
}
#endif // GPDXL_CParseHandlerAppend_H

// EOF
