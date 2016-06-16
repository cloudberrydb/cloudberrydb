//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalRowTrigger.h
//
//	@doc:
//		Parse handler for parsing a physical row trigger operator
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerPhysicalRowTrigger_H
#define GPDXL_CParseHandlerPhysicalRowTrigger_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalRowTrigger
	//
	//	@doc:
	//		Parse handler for parsing a physical row trigger operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalRowTrigger : public CParseHandlerPhysicalOp
	{
		private:

			CDXLPhysicalRowTrigger *m_pdxlop;

			// private copy ctor
			CParseHandlerPhysicalRowTrigger(const CParseHandlerPhysicalRowTrigger &);

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
			CParseHandlerPhysicalRowTrigger
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerPhysicalRowTrigger_H

// EOF
