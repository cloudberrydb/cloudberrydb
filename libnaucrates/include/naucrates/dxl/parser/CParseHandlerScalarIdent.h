//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarIdent.h
//
//	@doc:
//		SAX parse handler class for parsing scalar identifier nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarIdent_H
#define GPDXL_CParseHandlerScalarIdent_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarIdent.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarIdent
	//
	//	@doc:
	//		Parse handler for parsing a scalar identifier operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarIdent : public CParseHandlerScalarOp
	{
		private:

			// the scalar identifier
			CDXLScalarIdent *m_pdxlop;
			
			// private copy ctor
			CParseHandlerScalarIdent(const CParseHandlerScalarIdent&);
			
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
			CParseHandlerScalarIdent
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			virtual
			~CParseHandlerScalarIdent();
	};
}

#endif // !GPDXL_CParseHandlerScalarIdent_H

// EOF
