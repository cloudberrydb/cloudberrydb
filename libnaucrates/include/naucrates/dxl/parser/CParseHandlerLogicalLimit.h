//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp, Inc.
//
//	@filename:
//		CParseHandlerLogicalLimit.h
//
//	@doc:
//		Parse handler for parsing a logical limit operator
//		
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalLimit_H
#define GPDXL_CParseHandlerLogicalLimit_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"
#include "naucrates/dxl/operators/CDXLNode.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalLimit
	//
	//	@doc:
	//		Parse handler for parsing a logical limit operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalLimit : public CParseHandlerLogicalOp
	{
		private:

			// private copy ctor
			CParseHandlerLogicalLimit(const CParseHandlerLogicalLimit &);

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
			CParseHandlerLogicalLimit
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerLogicalLimit_H

// EOF
