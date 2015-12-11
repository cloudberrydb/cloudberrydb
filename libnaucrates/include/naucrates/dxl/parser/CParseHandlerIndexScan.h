//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerIndexScan.h
//
//	@doc:
//		SAX parse handler class for parsing index scan operator nodes
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerIndexScan_H
#define GPDXL_CParseHandlerIndexScan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/operators/CDXLPhysicalIndexScan.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerIndexScan
	//
	//	@doc:
	//		Parse handler for index scan operator nodes
	//
	//---------------------------------------------------------------------------
	class CParseHandlerIndexScan : public CParseHandlerPhysicalOp
	{
		private:

			// index scan direction
			EdxlIndexScanDirection m_edxlisd;

			// private copy ctor
			CParseHandlerIndexScan(const CParseHandlerIndexScan &);

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

		protected:

			// common StartElement functionality for IndexScan and IndexOnlyScan
			void StartElementHelper
				(
				const XMLCh* const xmlszLocalname,
				const Attributes& attrs,
				Edxltoken edxltoken
				);

			// common EndElement functionality for IndexScan and IndexOnlyScan
			void EndElementHelper
				(
				const XMLCh* const xmlszLocalname,
				Edxltoken edxltoken,
				ULONG ulPartIndexId = 0,
				ULONG ulPartIndexIdPrintable = 0
				);

		public:
			// ctor
			CParseHandlerIndexScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerIndexScan_H

// EOF
