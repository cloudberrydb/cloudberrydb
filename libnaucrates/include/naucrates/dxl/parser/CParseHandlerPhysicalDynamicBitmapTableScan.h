//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerPhysicalDynamicBitmapTableScan.h
//
//	@doc:
//		SAX parse handler class for parsing dynamic bitmap table scan operator nodes
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerPhysicalDynamicBitmapTableScan_H
#define GPDXL_CParseHandlerPhysicalDynamicBitmapTableScan_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalAbstractBitmapScan.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalDynamicBitmapTableScan
	//
	//	@doc:
	//		Parse handler for parsing dynamic bitmap table scan operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalDynamicBitmapTableScan : public CParseHandlerPhysicalAbstractBitmapScan
	{
		private:
			// private copy ctor
			CParseHandlerPhysicalDynamicBitmapTableScan(const CParseHandlerPhysicalDynamicBitmapTableScan &);

			// part index id
			ULONG m_ulPartIndexId;

			// printable partition index id
			ULONG m_ulPartIndexIdPrintable;

			// process the start of an element
			virtual
			void StartElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
				);

			// process the end of an element
			virtual
			void EndElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
				);

		public:
			// ctor
			CParseHandlerPhysicalDynamicBitmapTableScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				)
				:
				CParseHandlerPhysicalAbstractBitmapScan(pmp, pphm, pphRoot),
				m_ulPartIndexId(0),
				m_ulPartIndexIdPrintable(0)
			{}
	};
}

#endif  // !GPDXL_CParseHandlerPhysicalDynamicBitmapTableScan_H

// EOF
