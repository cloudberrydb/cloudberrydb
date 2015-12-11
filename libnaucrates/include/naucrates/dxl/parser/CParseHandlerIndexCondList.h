//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerIndexCondList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of index conditions in a
//		index scan node
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerIndexCondList_H
#define GPDXL_CParseHandlerIndexCondList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerIndexCondList
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of index conditions in a
	//		index scan node
	//
	//---------------------------------------------------------------------------
	class CParseHandlerIndexCondList : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerIndexCondList(const CParseHandlerIndexCondList&);

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
			CParseHandlerIndexCondList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerIndexCondList_H

// EOF
