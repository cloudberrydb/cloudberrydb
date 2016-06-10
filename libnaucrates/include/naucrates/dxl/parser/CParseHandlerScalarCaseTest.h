//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarCaseTest.h
//
//	@doc:
//
//		SAX parse handler class for parsing case test
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerScalarCaseTest_H
#define GPDXL_CParseHandlerScalarCaseTest_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLScalarCaseTest.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarCaseTest
	//
	//	@doc:
	//		Parse handler for parsing a case test
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarCaseTest : public CParseHandlerScalarOp
	{
		private:

			// return type
			IMDId *m_pmdidType;

			// private copy ctor
			CParseHandlerScalarCaseTest(const CParseHandlerScalarCaseTest &);

			// process the start of an element
			void StartElement
					(
					const XMLCh* const xmlszUri,
					const XMLCh* const xmlszLocalname,
					const XMLCh* const xmlszQname,
					const Attributes& attr
					);

			// process the end of an element
			void EndElement
					(
					const XMLCh* const xmlszUri,
					const XMLCh* const xmlszLocalname,
					const XMLCh* const xmlszQname
					);

		public:
			// ctor
			CParseHandlerScalarCaseTest
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

		};
}

#endif // !GPDXL_CParseHandlerScalarCaseTest_H

//EOF
