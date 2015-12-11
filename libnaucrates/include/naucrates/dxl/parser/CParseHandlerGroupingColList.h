//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerGroupingColList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of grouping columns ids in an
//		aggregate operator node.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerGroupingColList_H
#define GPDXL_CParseHandlerGroupingColList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerGroupingColList
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of grouping column ids in 
	//		an Aggregate operator node.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerGroupingColList : public CParseHandlerBase
	{
		private:
			
			// array of grouping column ids
			DrgPul *m_pdrgpulGroupingCols;
		
			// private copy ctor
			CParseHandlerGroupingColList(const CParseHandlerGroupingColList&); 
		
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
			CParseHandlerGroupingColList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			virtual ~CParseHandlerGroupingColList();
			
			// accessor
			DrgPul *PdrgpulGroupingCols();
	};
}

#endif // !GPDXL_CParseHandlerGroupingColList_H

// EOF
