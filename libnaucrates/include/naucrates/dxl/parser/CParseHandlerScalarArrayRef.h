//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarArrayRef.h
//
//	@doc:
//		SAX parse handler class for parsing scalar arrayref
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarScalarArrayRef_H
#define GPDXL_CParseHandlerScalarScalarArrayRef_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarArrayRef
	//
	//	@doc:
	//		Parse handler class for parsing scalar arrayref
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarArrayRef : public CParseHandlerScalarOp
	{
		private:

			// number of index lists parsed
			ULONG m_ulIndexLists;

			// whether the parser is currently parsing the ref expr
			BOOL m_fParsingRefExpr;

			// whether the parser is currently parsing the assign expr
			BOOL m_fParsingAssignExpr;

			// private copy ctor
			CParseHandlerScalarArrayRef(const CParseHandlerScalarArrayRef&);

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
			CParseHandlerScalarArrayRef
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarScalarArrayRef_H

// EOF
