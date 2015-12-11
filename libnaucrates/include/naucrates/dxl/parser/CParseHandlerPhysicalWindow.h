//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalWindow.h
//
//	@doc:
//		SAX parse handler class for parsing a physical window operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerPhysicalWindow_H
#define GPDXL_CParseHandlerPhysicalWindow_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/operators/CDXLWindowKey.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalWindow
	//
	//	@doc:
	//		Parse handler for parsing a physical window operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalWindow : public CParseHandlerPhysicalOp
	{
		private:

			// array of partition columns used by the window functions
			DrgPul *m_pdrgpulPartCols;

			// private copy ctor
			CParseHandlerPhysicalWindow(const CParseHandlerPhysicalWindow &);

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
			CParseHandlerPhysicalWindow
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerPhysicalWindow_H

// EOF
