//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerColDescr.h
//
//	@doc:
//		SAX parse handler class for parsing the list of column descriptors 
//		in a table descriptor node.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerColumnDescriptor_H
#define GPDXL_CParseHandlerColumnDescriptor_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "naucrates/dxl/operators/CDXLColDescr.h"

namespace gpdxl
{
	using namespace gpos;


	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerColDescr
	//
	//	@doc:
	//		Parse handler for column descriptor lists
	//
	//---------------------------------------------------------------------------
	class CParseHandlerColDescr : public CParseHandlerBase
	{
		private:
					
			// array of column descriptors to build
			DrgPdxlcd *m_pdrgdxlcd;
			
			// current column descriptor being parsed
			CDXLColDescr *m_pdxlcd;
				
			// private copy ctor
			CParseHandlerColDescr(const CParseHandlerColDescr&); 
			
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
			CParseHandlerColDescr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			virtual ~CParseHandlerColDescr();
			
			DrgPdxlcd *Pdrgpdxlcd();
	};
}

#endif // !GPDXL_CParseHandlerColumnDescriptor_H

// EOF
