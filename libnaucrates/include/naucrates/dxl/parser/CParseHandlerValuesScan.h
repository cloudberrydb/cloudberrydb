//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerValuesScan.h
//
//	@doc:
//		SAX parse handler class for parsing ValuesScan operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerValuesScan_H
#define GPDXL_CParseHandlerValuesScan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalValuesScan.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerValuesScan
	//
	//	@doc:
	//		Parse handler for parsing a ValuesScan operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerValuesScan : public CParseHandlerPhysicalOp
	{
	private:

		// the ValuesScan operator
		CDXLPhysicalValuesScan *m_pdxlop;

		// private copy ctor
		CParseHandlerValuesScan(const CParseHandlerValuesScan &);

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
		CParseHandlerValuesScan
		(
		 IMemoryPool *pmp,
		 CParseHandlerManager *pphm,
		 CParseHandlerBase *pphRoot
		 );
	};
}

#endif // !GPDXL_CParseHandlerValuesScan_H

// EOF
