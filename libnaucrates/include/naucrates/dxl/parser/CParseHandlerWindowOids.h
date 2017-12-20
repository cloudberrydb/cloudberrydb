//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerWindowOids.h
//
//	@doc:
//		SAX parse handler class for parsing window function oids
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerWindowOids_H
#define GPDXL_CParseHandlerWindowOids_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "gpopt/base/CWindowOids.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerWindowOids
	//
	//	@doc:
	//		SAX parse handler class for parsing window function oids
	//
	//---------------------------------------------------------------------------
	class CParseHandlerWindowOids : public CParseHandlerBase
	{
		private:

			// deafult oids
			CWindowOids *m_pwindowoids;

			// private copy ctor
			CParseHandlerWindowOids(const CParseHandlerWindowOids&);

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
			CParseHandlerWindowOids
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual
			~CParseHandlerWindowOids();

			// type of the parse handler
			virtual
			EDxlParseHandlerType Edxlphtype() const;

			// return system specific window oids
			CWindowOids *Pwindowoids() const;
	};
}

#endif // !GPDXL_CParseHandlerWindowOids_H

// EOF
