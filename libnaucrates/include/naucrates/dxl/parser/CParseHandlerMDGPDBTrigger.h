//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBTrigger.h
//
//	@doc:
//		SAX parse handler class for GPDB trigger metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDGPDBTrigger_H
#define GPDXL_CParseHandlerMDGPDBTrigger_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"


#include "naucrates/md/CMDTriggerGPDB.h"


namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDGPDBTrigger
	//
	//	@doc:
	//		Parse handler for GPDB trigger metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDGPDBTrigger : public CParseHandlerMetadataObject
	{
		private:
			// trigger id
			IMDId *m_pmdid;

			// trigger name
			CMDName *m_pmdname;

			// relation id
			IMDId *m_pmdidRel;

			// function id
			IMDId *m_pmdidFunc;

			// trigger type
			INT m_iType;

			// is trigger enabled
			BOOL m_fEnabled;

			// private copy ctor
			CParseHandlerMDGPDBTrigger(const CParseHandlerMDGPDBTrigger &);

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
			CParseHandlerMDGPDBTrigger
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerMDGPDBTrigger_H

// EOF
