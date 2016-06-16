//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarAssertConstraintList.h
//
//	@doc:
//		SAX parse handler class for parsing assert constraint list nodes
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarAssertConstraintList_H
#define GPDXL_CParseHandlerScalarAssertConstraintList_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarAssertConstraintList
	//
	//	@doc:
	//		Parse handler for DXL scalar assert constraint lists 
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarAssertConstraintList : public CParseHandlerScalarOp
	{
		private:
			
			// scalar assert operator
			CDXLScalarAssertConstraintList *m_pdxlop;
			
			// current assert constraint
			CDXLScalarAssertConstraint *m_pdxlopAssertConstraint;
			
			// array of assert constraint nodes parsed so far
			DrgPdxln *m_pdrgpdxlnAssertConstraints;
			
			// private copy ctor
			CParseHandlerScalarAssertConstraintList(const CParseHandlerScalarAssertConstraintList&);
			
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
			CParseHandlerScalarAssertConstraintList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarAssertConstraintList_H

// EOF
