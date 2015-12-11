//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerPhysicalBitmapTableScan.h
//
//	@doc:
//		SAX parse handler class for parsing dynamic bitmap table scan operator nodes
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerPhysicalBitmapTableScan_H
#define GPDXL_CParseHandlerPhysicalBitmapTableScan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalAbstractBitmapScan.h"

namespace gpdxl
{
	using namespace gpos;

	// forward declarations
	class CDXLPhysicalBitmapTableScan;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalBitmapTableScan
	//
	//	@doc:
	//		Parse handler for parsing bitmap table scan operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalBitmapTableScan : public CParseHandlerPhysicalAbstractBitmapScan
	{
		private:
			// private copy ctor
			CParseHandlerPhysicalBitmapTableScan(const CParseHandlerPhysicalBitmapTableScan &);

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
			CParseHandlerPhysicalBitmapTableScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				)
				:
				CParseHandlerPhysicalAbstractBitmapScan(pmp, pphm, pphRoot)
			{}
	};
}

#endif  // !GPDXL_CParseHandlerPhysicalBitmapTableScan_H

// EOF
