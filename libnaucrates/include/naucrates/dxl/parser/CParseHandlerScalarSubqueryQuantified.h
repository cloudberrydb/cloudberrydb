//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CParseHandlerScalarSubqueryQuantified.h
//
//	@doc:
//		SAX parse handler class for parsing scalar subquery ALL and ANY operator node.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarSubqueryAny_H
#define GPDXL_CParseHandlerScalarSubqueryAny_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarSubqueryAny.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarSubqueryQuantified
	//
	//	@doc:
	//		Parse handler for parsing scalar subquery ALL and ANY operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarSubqueryQuantified : public CParseHandlerScalarOp
	{
		private:
			// scalar subquery operator
			CDXLScalar *m_pdxlop;
			
			// private copy ctor
			CParseHandlerScalarSubqueryQuantified(const CParseHandlerScalarSubqueryQuantified &);

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
			CParseHandlerScalarSubqueryQuantified
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarSubqueryAny_H

// EOF
