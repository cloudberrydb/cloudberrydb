//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerRelStats.h
//
//	@doc:
//		SAX parse handler class for parsing base relation stats objects
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerRelStats_H
#define GPDXL_CParseHandlerRelStats_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerRelStats
	//
	//	@doc:
	//		Base parse handler class for base relation stats
	//
	//---------------------------------------------------------------------------
	class CParseHandlerRelStats : public CParseHandlerMetadataObject
	{
		private:
		
			// private copy ctor
			CParseHandlerRelStats(const CParseHandlerRelStats&);

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
			CParseHandlerRelStats
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerRelStats_H

// EOF
