//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerMDRelationCtas.h
//
//	@doc:
//		SAX parse handler class for CTAS relation metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDRelationCTAS_H
#define GPDXL_CParseHandlerMDRelationCTAS_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMDRelation.h"

#include "naucrates/md/CMDRelationExternalGPDB.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDRelationCtas
	//
	//	@doc:
	//		Parse handler for CTAS relation metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDRelationCtas : public CParseHandlerMDRelation
	{
		private:

			// vartypemod list
			DrgPi *m_pdrgpiVarTypeMod;

			// private copy ctor
			CParseHandlerMDRelationCtas(const CParseHandlerMDRelationCtas &);

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
			CParseHandlerMDRelationCtas
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerMDRelationCTAS_H

// EOF
