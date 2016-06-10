//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerLogicalProject.h
//
//	@doc:
//		Parse handler for parsing a logical project operator
//		
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalProject_H
#define GPDXL_CParseHandlerLogicalProject_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"
#include "naucrates/dxl/operators/CDXLLogicalProject.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalProject
	//
	//	@doc:
	//		Parse handler for parsing a logical project operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalProject : public CParseHandlerLogicalOp
	{
		private:


			// private copy ctor
			CParseHandlerLogicalProject(const CParseHandlerLogicalProject &);

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
			CParseHandlerLogicalProject
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			~CParseHandlerLogicalProject();
	};
}

#endif // !GPDXL_CParseHandlerLogicalProject_H

// EOF
