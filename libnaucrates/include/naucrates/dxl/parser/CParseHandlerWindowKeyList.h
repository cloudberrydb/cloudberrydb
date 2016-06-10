//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowKeyList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of window keys
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerWindowKeyList_H
#define GPDXL_CParseHandlerWindowKeyList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/dxl/operators/CDXLWindowKey.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerWindowKeyList
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of window keys
	//
	//---------------------------------------------------------------------------
	class CParseHandlerWindowKeyList : public CParseHandlerBase
	{
		private:

			// list of window keys
			DrgPdxlwk *m_pdrgpdxlwk;

			// private copy ctor
			CParseHandlerWindowKeyList(const CParseHandlerWindowKeyList&);

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
			CParseHandlerWindowKeyList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// list of window keys
			DrgPdxlwk *Pdrgpdxlwk() const
			{
				return m_pdrgpdxlwk;
			}
	};
}

#endif // !GPDXL_CParseHandlerWindowKeyList_H

// EOF
