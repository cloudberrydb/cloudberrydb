//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerLogicalOp.h
//
//	@doc:
//		SAX parse handler class for parsing logical operators.
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerLogicalOp_H
#define GPDXL_CParseHandlerLogicalOp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalOp
	//
	//	@doc:
	//		Parse handler for logical operators
	//
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalOp : public CParseHandlerOp
	{
		private:

			// private copy ctor
			CParseHandlerLogicalOp(const CParseHandlerLogicalOp&);

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
			CParseHandlerLogicalOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerLogicalOp_H

// EOF
