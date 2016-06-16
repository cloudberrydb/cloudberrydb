//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarConstValue.h
//
//	@doc:
//		SAX parse handler class for parsing scalar ConstVal node.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarConst_H
#define GPDXL_CParseHandlerScalarConst_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLScalarConstValue.h"

namespace gpdxl
{
	using namespace gpos;


	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarConstValue
	//
	//	@doc:
	//		Parse handler for parsing a scalar ConstValue
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarConstValue : public CParseHandlerScalarOp
	{
	private:

		// private copy ctor
		CParseHandlerScalarConstValue(const CParseHandlerScalarConstValue &);

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
		CParseHandlerScalarConstValue
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // GPDXL_CParseHandlerScalarConst_H

// EOF
