//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalTVF.h
//
//	@doc:
//
//		SAX parse handler class for parsing Logical TVF
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerLogicalTVF_H
#define GPDXL_CParseHandlerLogicalTVF_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalTVF
	//
	//	@doc:
	//		Parse handler for parsing a Logical TVF
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalTVF : public CParseHandlerLogicalOp
	{
		private:

			// catalog id of the function
			IMDId *m_pmdidFunc;

			// return type
			IMDId *m_pmdidRetType;

			// function name
			CMDName *m_pmdname;

			// private copy ctor
			CParseHandlerLogicalTVF(const CParseHandlerLogicalTVF &);

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
			CParseHandlerLogicalTVF
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);
	};

}
#endif // GPDXL_CParseHandlerLogicalTVF_H

// EOF
