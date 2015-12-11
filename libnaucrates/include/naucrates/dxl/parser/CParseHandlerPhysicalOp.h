//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerPhysicalOp.h
//
//	@doc:
//		SAX parse handler class for parsing physical operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerPhysicalOp_H
#define GPDXL_CParseHandlerPhysicalOp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalOp
	//
	//	@doc:
	//		Parse handler for physical operators
	//
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalOp : public CParseHandlerOp 
	{
		private:

			// private copy ctor
			CParseHandlerPhysicalOp(const CParseHandlerPhysicalOp&);
			
			
		protected:

			// process the start of an element
			virtual void StartElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
				);
				
			// process the end of an element
			virtual void EndElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
				);
						
		public:
			// ctor/dtor
			CParseHandlerPhysicalOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			virtual
			~CParseHandlerPhysicalOp();

	};
}

#endif // !GPDXL_CParseHandlerPhysicalOp_H

// EOF
