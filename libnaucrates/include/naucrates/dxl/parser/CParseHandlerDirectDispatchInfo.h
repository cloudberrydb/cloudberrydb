//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		CParseHandlerDirectDispatchInfo.h
//
//	@doc:
//		SAX parse handler class for parsing the direct dispatch info
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerDirectDispatchInfo_H
#define GPDXL_CParseHandlerDirectDispatchInfo_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	// fwd decl
	class CDXLDirectDispatchInfo;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerDirectDispatchInfo
	//
	//	@doc:
	//		Parse handler for direct dispatch info
	//
	//---------------------------------------------------------------------------
	class CParseHandlerDirectDispatchInfo : public CParseHandlerBase
	{
		private:

			// current array of datums being parsed
			DrgPdxldatum *m_pdrgpdxldatum;
			
			// array of datum combinations
			DrgPdrgPdxldatum *m_pdrgpdrgpdxldatum;
		
			// direct dispatch spec
			CDXLDirectDispatchInfo *m_pdxlddinfo;
			
			// private copy ctor
			CParseHandlerDirectDispatchInfo(const CParseHandlerDirectDispatchInfo &);
			
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
			CParseHandlerDirectDispatchInfo
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// dtor
			~CParseHandlerDirectDispatchInfo();
			
			// accessor to the parsed direct dispatch spec
			CDXLDirectDispatchInfo *Pdxlddinfo() const;
	};
}

#endif // !GPDXL_CParseHandlerDirectDispatchInfo_H

// EOF
