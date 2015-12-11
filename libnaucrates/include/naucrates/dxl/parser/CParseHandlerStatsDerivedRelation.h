//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStatsDerivedRelation.h
//
//	@doc:
//		SAX parse handler class for parsing derived relation statistics
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerStatsDerivedRelation_H
#define GPDXL_CParseHandlerStatsDerivedRelation_H

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
	//		CParseHandlerStatsDerivedRelation
	//
	//	@doc:
	//		Base parse handler class for derived relation statistics
	//
	//---------------------------------------------------------------------------
	class CParseHandlerStatsDerivedRelation : public CParseHandlerBase
	{
		private:

			// number of rows in the relation
			CDouble m_dRows;

			// flag to express that the statistics is on an empty input
			BOOL m_fEmpty;

			// relation stats
			CDXLStatsDerivedRelation *m_pdxlstatsderrel;

			// private copy ctor
			CParseHandlerStatsDerivedRelation(const CParseHandlerStatsDerivedRelation&);

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
			CParseHandlerStatsDerivedRelation
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual ~CParseHandlerStatsDerivedRelation();

			// the derived relation stats
			CDXLStatsDerivedRelation *Pdxlstatsderrel() const
			{
				return m_pdxlstatsderrel;
			}
	};
}

#endif // !GPDXL_CParseHandlerStatsDerivedRelation_H

// EOF
