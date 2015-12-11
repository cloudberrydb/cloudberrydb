//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerIndexOnlyScan.h
//
//	@doc:
//		SAX parse handler class for parsing index only scan operator nodes
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerIndexOnlyScan_H
#define GPDXL_CParseHandlerIndexOnlyScan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerIndexScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalIndexOnlyScan.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerIndexOnlyScan
	//
	//	@doc:
	//		Parse handler for index only scan operator nodes
	//
	//---------------------------------------------------------------------------
	class CParseHandlerIndexOnlyScan : public CParseHandlerIndexScan
	{
		private:

			// private copy ctor
			CParseHandlerIndexOnlyScan(const CParseHandlerIndexOnlyScan &);

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
			CParseHandlerIndexOnlyScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerIndexOnlyScan_H

// EOF
