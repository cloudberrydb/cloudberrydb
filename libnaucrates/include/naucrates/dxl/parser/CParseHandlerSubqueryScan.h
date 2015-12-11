//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerSubqueryScan.h
//
//	@doc:
//		SAX parse handler class for parsing subquery scan operator nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerSubqueryScan_H
#define GPDXL_CParseHandlerSubqueryScan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalSubqueryScan.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerSubqueryScan
	//
	//	@doc:
	//		Parse handler for parsing a subquery scan operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerSubqueryScan : public CParseHandlerPhysicalOp
	{
		private:
		
			// the subquery scan operator
			CDXLPhysicalSubqueryScan *m_pdxlop;
		
			// private copy ctor
			CParseHandlerSubqueryScan(const CParseHandlerSubqueryScan &);

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
			CParseHandlerSubqueryScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerSubqueryScan_H

// EOF
