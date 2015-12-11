//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerSortColList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of sorting columns in sort
//		and motion nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerSortColList_H
#define GPDXL_CParseHandlerSortColList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerSortColList
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of sorting columns 
	//		in sort and motion nodes.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerSortColList : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerSortColList(const CParseHandlerSortColList&); 
		
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
			CParseHandlerSortColList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

	};
}

#endif // !GPDXL_CParseHandlerSortColList_H

// EOF
