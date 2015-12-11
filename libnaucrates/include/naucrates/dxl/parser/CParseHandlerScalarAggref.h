//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarAggref.h
//
//	@doc:
//		
//		SAX parse handler class for parsing scalar AggRef.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarAggref_H
#define GPDXL_CParseHandlerScalarAggref_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarAggref.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarAggref
	//
	//	@doc:
	//		Parse handler for parsing a scalar aggexpr expression
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarAggref : public CParseHandlerScalarOp
	{
		private:
	
			// private copy ctor
			CParseHandlerScalarAggref(const CParseHandlerScalarAggref &);
	
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
			CParseHandlerScalarAggref
			(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
			);


	};

}
#endif // !GPDXL_CParseHandlerScalarAggref_H

//EOF
