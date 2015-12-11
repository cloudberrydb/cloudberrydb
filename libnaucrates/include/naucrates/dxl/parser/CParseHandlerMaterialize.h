//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMaterialize.h
//
//	@doc:
//		SAX parse handler class for parsing materialize operator nodes.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMaterialize_H
#define GPDXL_CParseHandlerMaterialize_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalMaterialize.h"


namespace gpdxl
{
	using namespace gpos;


	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMaterialize
	//
	//	@doc:
	//		Parse handler for parsing a materialize operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMaterialize : public CParseHandlerPhysicalOp
	{
		private:

			// the materialize operator
			CDXLPhysicalMaterialize *m_pdxlop;

			// private copy ctor
			CParseHandlerMaterialize(const CParseHandlerMaterialize &);

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
			CParseHandlerMaterialize
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerMaterialize_H

// EOF
