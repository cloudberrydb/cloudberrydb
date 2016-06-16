//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowSpecList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of window specifications
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerWindowSpecList_H
#define GPDXL_CParseHandlerWindowSpecList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/dxl/operators/CDXLWindowSpec.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerWindowSpecList
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of window specifications
	//
	//---------------------------------------------------------------------------
	class CParseHandlerWindowSpecList : public CParseHandlerBase
	{
		private:

			// list of window specifications
			DrgPdxlws *m_pdrgpdxlws;

			// private copy ctor
			CParseHandlerWindowSpecList(const CParseHandlerWindowSpecList&);

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
			CParseHandlerWindowSpecList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// list of window keys
			DrgPdxlws *Pdrgpdxlws() const
			{
				return m_pdrgpdxlws;
			}
	};
}

#endif // !GPDXL_CParseHandlerWindowSpecList_H

// EOF
