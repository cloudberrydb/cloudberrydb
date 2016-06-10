//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerEnumeratorConfig.h
//
//	@doc:
//		SAX parse handler class for parsing enumerator configuration options
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerEnumeratorConfig_H
#define GPDXL_CParseHandlerEnumeratorConfig_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerEnumeratorConfig
	//
	//	@doc:
	//		SAX parse handler class for parsing enumerator configuration options
	//
	//---------------------------------------------------------------------------
	class CParseHandlerEnumeratorConfig : public CParseHandlerBase
	{
		private:

			// enumerator configuration
			CEnumeratorConfig *m_pec;

			// private copy ctor
			CParseHandlerEnumeratorConfig(const CParseHandlerEnumeratorConfig&);

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
			CParseHandlerEnumeratorConfig
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual
			~CParseHandlerEnumeratorConfig();

			// type of the parse handler
			EDxlParseHandlerType Edxlphtype() const;

			// enumerator configuration
			CEnumeratorConfig *Pec() const;
	};
}

#endif // !GPDXL_CParseHandlerEnumeratorConfig_H

// EOF
