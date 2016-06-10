//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerLimit.h
//
//	@doc:
//		SAX parse handler class for parsing limit operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerLimit_H
#define GPDXL_CParseHandlerLimit_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/operators/CDXLPhysicalLimit.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLimit
	//
	//	@doc:
	//		Parse handler for limit operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLimit : public CParseHandlerPhysicalOp
	{
		private:

				CDXLPhysicalLimit *m_pdxlop;

				// private copy ctor
				CParseHandlerLimit(const CParseHandlerLimit &);

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
				CParseHandlerLimit
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);
	};
}
#endif // GPDXL_CParseHandlerLimit_H

// EOF
