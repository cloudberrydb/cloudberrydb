//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerCondList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of conditions in a 
//		hash join or merge join node.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerCondList_H
#define GPDXL_CParseHandlerCondList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerCondList
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of conditions in a 
	//		hash join or merge join node.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerCondList : public CParseHandlerScalarOp
	{
		private:

		
			// private copy ctor
			CParseHandlerCondList(const CParseHandlerCondList&); 
		
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
			CParseHandlerCondList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
	};
}

#endif // !GPDXL_CParseHandlerCondList_H

// EOF
