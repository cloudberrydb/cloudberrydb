//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarIfStmt.h
//
//	@doc:
//		
//		SAX parse handler class for parsing If statement operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerIfStmt_H
#define GPDXL_CParseHandlerIfStmt_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLScalarIfStmt.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarIfStmt
	//
	//	@doc:
	//		Parse handler for parsing an IF statement
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarIfStmt : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarIfStmt(const CParseHandlerScalarIfStmt &);

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
			CParseHandlerScalarIfStmt
						(
						IMemoryPool *pmp,
						CParseHandlerManager *pphm,
						CParseHandlerBase *pphRoot
						);

		};
}

#endif // !GPDXL_CParseHandlerIfStmt_H

//EOF
