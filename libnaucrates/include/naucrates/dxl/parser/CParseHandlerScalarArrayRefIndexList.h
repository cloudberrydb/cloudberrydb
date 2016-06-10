//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarArrayRefIndexList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of arrayref indexes
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarScalarArrayRefIndexList_H
#define GPDXL_CParseHandlerScalarScalarArrayRefIndexList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarArrayRefIndexList
	//
	//	@doc:
	//		Parse handler class for parsing the list of arrayref indexes
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarArrayRefIndexList : public CParseHandlerScalarOp
	{
		private:
			
			// private copy ctor
			CParseHandlerScalarArrayRefIndexList(const CParseHandlerScalarArrayRefIndexList&);
		
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
			CParseHandlerScalarArrayRefIndexList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarScalarArrayRefIndexList_H

// EOF
