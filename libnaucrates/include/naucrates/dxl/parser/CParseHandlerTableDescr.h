//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerTableDescr.h
//
//	@doc:
//		SAX parse handler class for parsing table descriptor portions
//		of scan operator nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerTableDescriptor_H
#define GPDXL_CParseHandlerTableDescriptor_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "naucrates/dxl/operators/CDXLTableDescr.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerTableDescr
	//
	//	@doc:
	//		Parse handler for parsing a table descriptor
	//
	//---------------------------------------------------------------------------
	class CParseHandlerTableDescr : public CParseHandlerBase
	{
		private:

			// the table descriptor to construct
			CDXLTableDescr *m_pdxltabdesc;
				
			// private copy ctor
			CParseHandlerTableDescr(const CParseHandlerTableDescr &);
			
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
			CParseHandlerTableDescr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			~CParseHandlerTableDescr();
		
			CDXLTableDescr *Pdxltabdesc();
			
	};
}

#endif // !GPDXL_CParseHandlerTableDescriptor_H

// EOF
