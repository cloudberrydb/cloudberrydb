//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CParseHandlerScalarSubqueryExists.h
//
//	@doc:
//		SAX parse handler class for parsing EXISTS and NOT EXISTS subquery operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarSubqueryExists_H
#define GPDXL_CParseHandlerScalarSubqueryExists_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarSubqueryExists.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarSubqueryExists
	//
	//	@doc:
	//		Parse handler for parsing EXISTS and NOT EXISTS subquery operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarSubqueryExists : public CParseHandlerScalarOp
	{
		private:
			// scalar subquery operator
			CDXLScalar *m_pdxlop;
			
			// private copy ctor
			CParseHandlerScalarSubqueryExists(const CParseHandlerScalarSubqueryExists &);
									
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
			// ctor/dtor
			CParseHandlerScalarSubqueryExists
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarSubqueryExists_H

// EOF
