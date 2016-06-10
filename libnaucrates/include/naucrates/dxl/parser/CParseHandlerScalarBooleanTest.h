//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarBooleanTest.h
//
//	@doc:
//		
//		SAX parse handler class for parsing scalar BooleanTest.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarBooleanTest_H
#define GPDXL_CParseHandlerScalarBooleanTest_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLScalarBooleanTest.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarBooleanTest
	//
	//	@doc:
	//		Parse handler for parsing a scalar boolean test
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarBooleanTest : public CParseHandlerScalarOp
	{
		private:
	
			EdxlBooleanTestType m_edxlBooleanTestType;
	
			// private copy ctor
			CParseHandlerScalarBooleanTest(const CParseHandlerScalarBooleanTest &);
	
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
	
			// parse the boolean test type from the Xerces xml string
			static
			EdxlBooleanTestType EdxlBooleantestType(const XMLCh *xmlszBoolType);

		public:
			// ctor
			CParseHandlerScalarBooleanTest
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

	};

}
#endif // GPDXL_CParseHandlerScalarBooleanTest_H

//EOF
