//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerProjList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of projection elements
//		in a DXL physical operator node.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarProjList_H
#define GPDXL_CParseHandlerScalarProjList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerProjList
	//
	//	@doc:
	//		Parse handler class for parsing the list of projection elements
	//		in a DXL physical operator node.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerProjList : public CParseHandlerScalarOp
	{
		private:
			
			// private copy ctor
			CParseHandlerProjList(const CParseHandlerProjList&); 
		
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
			CParseHandlerProjList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarProjList_H

// EOF
