//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStatistics.h
//
//	@doc:
//		SAX parse handler class for parsing statistics from a DXL document.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerStatistics_H
#define GPDXL_CParseHandlerStatistics_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/md/CDXLStatsDerivedRelation.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerStatistics
	//
	//	@doc:
	//		Parse handler for statistics.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerStatistics : public CParseHandlerBase
	{
		private:

			// list of derived table statistics
			DrgPdxlstatsderrel *m_pdrgpdxlstatsderrel;

			// private copy ctor
			CParseHandlerStatistics(const CParseHandlerStatistics&);

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
			CParseHandlerStatistics
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			~CParseHandlerStatistics();

			virtual
			EDxlParseHandlerType Edxlphtype() const;

			// return the list of statistics objects
			DrgPdxlstatsderrel *Pdrgpdxlstatsderrel() const;

	};
}

#endif // !GPDXL_CParseHandlerStatistics_H

// EOF
