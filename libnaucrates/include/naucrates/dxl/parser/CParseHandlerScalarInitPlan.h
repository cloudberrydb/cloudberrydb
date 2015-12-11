//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarInitPlan.h
//
//	@doc:
//		
//		SAX parse handler class for parsing InitPlan.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarInitPlan_H
#define GPDXL_CParseHandlerScalarInitPlan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarInitPlan.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarInitPlan
	//
	//	@doc:
	//		Parse handler for parsing a scalar Init Plan
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarInitPlan : public CParseHandlerScalarOp
	{
		private:
	
			// private copy ctor
			CParseHandlerScalarInitPlan(const CParseHandlerScalarInitPlan &);
	
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
			CParseHandlerScalarInitPlan
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);
	};

}
#endif // GPDXL_CParseHandlerScalarInitPlan_H

//EOF
