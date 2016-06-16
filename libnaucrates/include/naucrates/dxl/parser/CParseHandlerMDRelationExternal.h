//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerMDRelationExternal.h
//
//	@doc:
//		SAX parse handler class for external relation metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDRelationExternal_H
#define GPDXL_CParseHandlerMDRelationExternal_H

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
	//		CParseHandlerMDRelationExternal
	//
	//	@doc:
	//		Parse handler for external relation metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDRelationExternal : public CParseHandlerMDRelation
	{
		private:

			// reject limit
			INT m_iRejectLimit;

			// reject limit in rows?
			BOOL m_fRejLimitInRows;

			// format error table mdid
			IMDId *m_pmdidFmtErrRel;

			// private copy ctor
			CParseHandlerMDRelationExternal(const CParseHandlerMDRelationExternal &);

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
			CParseHandlerMDRelationExternal
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerMDRelationExternal_H

// EOF
