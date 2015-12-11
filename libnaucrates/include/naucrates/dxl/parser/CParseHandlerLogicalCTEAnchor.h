//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalCTEAnchor.h
//
//	@doc:
//		Parse handler for parsing a logical CTE anchor operator
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalCTEAnchor_H
#define GPDXL_CParseHandlerLogicalCTEAnchor_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalCTEAnchor
	//
	//	@doc:
	//		Parse handler for parsing a logical CTE anchor operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalCTEAnchor : public CParseHandlerLogicalOp
	{
		private:

			// private copy ctor
			CParseHandlerLogicalCTEAnchor(const CParseHandlerLogicalCTEAnchor &);

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
			CParseHandlerLogicalCTEAnchor
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerLogicalCTEAnchor_H

// EOF
