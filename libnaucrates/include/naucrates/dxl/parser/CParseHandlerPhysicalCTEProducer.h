//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalCTEProducer.h
//
//	@doc:
//		Parse handler for parsing a physical CTE producer operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerPhysicalCTEProducer_H
#define GPDXL_CParseHandlerPhysicalCTEProducer_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalCTEProducer
	//
	//	@doc:
	//		Parse handler for parsing a physical CTE producer operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalCTEProducer : public CParseHandlerPhysicalOp
	{
		private:

			// private copy ctor
			CParseHandlerPhysicalCTEProducer(const CParseHandlerPhysicalCTEProducer &);

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
			CParseHandlerPhysicalCTEProducer
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerPhysicalCTEProducer_H

// EOF
