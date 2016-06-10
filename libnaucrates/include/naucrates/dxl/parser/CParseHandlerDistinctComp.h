//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerDistinctComp.h
//
//	@doc:
//		SAX parse handler class for parsing distinct comparison nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerDistinctComp_H
#define GPDXL_CParseHandlerDistinctComp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarDistinctComp.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerDistinctComp
	//
	//	@doc:
	//		Parse handler for parsing a distinct comparison operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerDistinctComp : public CParseHandlerScalarOp
	{
		private:

			
			// the distinct comparison operator
			CDXLScalarDistinctComp *m_pdxlop;

			// private copy ctor
			CParseHandlerDistinctComp(const CParseHandlerDistinctComp &);
			
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
			CParseHandlerDistinctComp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerDistinctComp_H

// EOF
