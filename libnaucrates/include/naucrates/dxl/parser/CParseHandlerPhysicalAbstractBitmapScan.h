//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerPhysicalAbstractBitmapScan.h
//
//	@doc:
//		SAX parse handler parent class for parsing bitmap scan operator nodes
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerPhysicalAbstractBitmapScan_H
#define GPDXL_CParseHandlerPhysicalAbstractBitmapScan_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalAbstractBitmapScan
	//
	//	@doc:
	//		Parse handler parent class for parsing bitmap scan operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalAbstractBitmapScan : public CParseHandlerPhysicalOp
	{
		private:
			// private copy ctor
			CParseHandlerPhysicalAbstractBitmapScan(const CParseHandlerPhysicalAbstractBitmapScan &);

		protected:
			// common StartElement functionality for child classes
			void StartElementHelper
				(
				const XMLCh* const xmlszLocalname,
				Edxltoken edxltoken
				);

			// common EndElement functionality for child classes
			void EndElementHelper
				(
				const XMLCh* const xmlszLocalname,
				Edxltoken edxltoken,
				ULONG ulPartIndexId = 0,
				ULONG ulPartIndexIdPrintable = 0
				);

		public:
			// ctor
			CParseHandlerPhysicalAbstractBitmapScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				)
				:
				CParseHandlerPhysicalOp(pmp, pphm, pphRoot)
			{}
	};
}

#endif  // !GPDXL_CParseHandlerPhysicalAbstractBitmapScan_H

// EOF
