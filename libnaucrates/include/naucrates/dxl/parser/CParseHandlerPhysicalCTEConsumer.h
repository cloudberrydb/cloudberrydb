//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalCTEConsumer.h
//
//	@doc:
//		Parse handler for parsing a physical CTE Consumer operator
//		
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerPhysicalCTEConsumer_H
#define GPDXL_CParseHandlerPhysicalCTEConsumer_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalCTEConsumer
	//
	//	@doc:
	//		Parse handler for parsing a physical CTE Consumer operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalCTEConsumer : public CParseHandlerPhysicalOp
	{
		private:

			// private copy ctor
			CParseHandlerPhysicalCTEConsumer(const CParseHandlerPhysicalCTEConsumer &);

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
			CParseHandlerPhysicalCTEConsumer
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerPhysicalCTEConsumer_H

// EOF
