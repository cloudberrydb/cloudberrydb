//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerSharedScan.h
//
//	@doc:
//		SAX parse handler class for parsing shared scan operator nodes.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerSharedScan_H
#define GPDXL_CParseHandlerSharedScan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalSharedScan.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerSharedScan
	//
	//	@doc:
	//		Parse handler for parsing a shared scan operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerSharedScan : public CParseHandlerPhysicalOp
	{
		private:

			// the shared scan operator
			CDXLPhysicalSharedScan *m_pdxlop;

			// private copy ctor
			CParseHandlerSharedScan(const CParseHandlerSharedScan &);

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
			CParseHandlerSharedScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerSharedScan_H

// EOF
