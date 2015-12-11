//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerSequence.h
//
//	@doc:
//		SAX parse handler class for parsing sequence operator nodes
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerSequence_H
#define GPDXL_CParseHandlerSequence_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	// fwd decl
	class CDXLPhysicalDynamicTableScan;

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerSequence
	//
	//	@doc:
	//		Parse handler for parsing a sequence operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerSequence : public CParseHandlerPhysicalOp
	{
		private:
			
			// are we already inside a sequence operator
			BOOL m_fInsideSequence;

			// private copy ctor
			CParseHandlerSequence(const CParseHandlerSequence &);

			// process the start of an element
			virtual
			void StartElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
				);
				
			// process the end of an element
			virtual
			void EndElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
				);
			
		public:
			
			// ctor
			CParseHandlerSequence(IMemoryPool *pmp, CParseHandlerManager *pphm, CParseHandlerBase *pph);
	};
}

#endif // !GPDXL_CParseHandlerSequence_H

// EOF
