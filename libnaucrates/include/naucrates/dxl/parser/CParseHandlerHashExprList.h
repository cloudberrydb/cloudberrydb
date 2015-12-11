//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerHashExprList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of hash expressions in a 
//		redistribute motion node.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerHashExprList_H
#define GPDXL_CParseHandlerHashExprList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerHashExprList
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of hash expressions in a 
	//		redistribute motion node.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerHashExprList : public CParseHandlerScalarOp
	{
		private:

		
			// private copy ctor
			CParseHandlerHashExprList(const CParseHandlerHashExprList&); 
		
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
			CParseHandlerHashExprList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerHashExprList_H

// EOF
