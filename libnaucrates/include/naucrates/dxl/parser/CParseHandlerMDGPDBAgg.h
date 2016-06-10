//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBAgg.h
//
//	@doc:
//		SAX parse handler class for GPDB aggregate metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDGPDBAgg_H
#define GPDXL_CParseHandlerMDGPDBAgg_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"


#include "naucrates/md/CMDAggregateGPDB.h"


namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDGPDBAgg
	//
	//	@doc:
	//		Parse handler for GPDB aggregate metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDGPDBAgg : public CParseHandlerMetadataObject
	{
		private:
			// metadata id comprising of id and version info.
			IMDId *m_pmdid;
			
			// name
			CMDName *m_pmdname;
					
			// result type
			IMDId *m_pmdidTypeResult;
			
			// intermediate result type
			IMDId *m_pmdidTypeIntermediate;
						
			// is aggregate ordered
			BOOL m_fOrdered;
			
			// is aggregate splittable
			BOOL m_fSplittable;
			
			// can we use hash aggregation to compute agg function
			BOOL m_fHashAggCapable;

			// private copy ctor
			CParseHandlerMDGPDBAgg(const CParseHandlerMDGPDBAgg &);
			
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
			CParseHandlerMDGPDBAgg
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);			
	};
}

#endif // !GPDXL_CParseHandlerMDGPDBAgg_H

// EOF
