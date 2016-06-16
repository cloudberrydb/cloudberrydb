//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerLogicalJoin.h
//
//	@doc:
//		Parse handler for parsing a logical join operator
//		
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalJoin_H
#define GPDXL_CParseHandlerLogicalJoin_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"
#include "naucrates/dxl/operators/CDXLLogicalJoin.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalJoin
	//
	//	@doc:
	//		Parse handler for parsing a logical join operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalJoin : public CParseHandlerLogicalOp
	{
		private:

			// private copy ctor
			CParseHandlerLogicalJoin(const CParseHandlerLogicalJoin &);

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
			CParseHandlerLogicalJoin
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			~CParseHandlerLogicalJoin();
	};
}

#endif // !GPDXL_CParseHandlerLogicalJoin_H

// EOF
