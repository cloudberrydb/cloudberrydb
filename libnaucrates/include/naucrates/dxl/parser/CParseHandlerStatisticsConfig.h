//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerStatisticsConfig.h
//
//	@doc:
//		SAX parse handler class for parsing statistics configuration
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerStatisticsConfig_H
#define GPDXL_CParseHandlerStatisticsConfig_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerStatisticsConfig
	//
	//	@doc:
	//		SAX parse handler class for parsing statistics configuration options
	//
	//---------------------------------------------------------------------------
	class CParseHandlerStatisticsConfig : public CParseHandlerBase
	{
		private:

			// statistics configuration
			CStatisticsConfig *m_pstatsconf;

			// private copy ctor
			CParseHandlerStatisticsConfig(const CParseHandlerStatisticsConfig&);

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
			CParseHandlerStatisticsConfig
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual
			~CParseHandlerStatisticsConfig();

			// type of the parse handler
			virtual
			EDxlParseHandlerType Edxlphtype() const;

			// enumerator configuration
			CStatisticsConfig *Pstatsconf() const;
	};
}

#endif // !GPDXL_CParseHandlerStatisticsConfig_H

// EOF
