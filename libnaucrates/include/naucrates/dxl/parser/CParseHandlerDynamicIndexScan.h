//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerDynamicIndexScan.h
//
//	@doc:
//		SAX parse handler class for parsing dynamic index scan operator nodes
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerDynamicIndexScan_H
#define GPDXL_CParseHandlerDynamicIndexScan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerIndexScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalIndexScan.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerDynamicIndexScan
	//
	//	@doc:
	//		Parse handler for index scan operator nodes
	//
	//---------------------------------------------------------------------------
	class CParseHandlerDynamicIndexScan : public CParseHandlerIndexScan
	{
		private:

			// part index id
			ULONG m_ulPartIndexId;
			
			// printable partition index id
			ULONG m_ulPartIndexIdPrintable;

			// private copy ctor
			CParseHandlerDynamicIndexScan(const CParseHandlerDynamicIndexScan &);

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
			CParseHandlerDynamicIndexScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerDynamicIndexScan_H

// EOF
