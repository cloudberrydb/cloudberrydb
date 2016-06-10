//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarLimitOffset.h
//
//	@doc:
//		SAX parse handler class for parsing LimitOffset
//		
//---------------------------------------------------------------------------


#ifndef GPDXL_CParseHandlerScalarLimitOffset_H
#define GPDXL_CParseHandlerScalarLimitOffset_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarLimitOffset
	//
	//	@doc:
	//		Parse handler for parsing a LIMIT Offset statement
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarLimitOffset : public CParseHandlerScalarOp
	{
		private:
			// private copy ctor
			CParseHandlerScalarLimitOffset(const CParseHandlerScalarLimitOffset &);

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
			CParseHandlerScalarLimitOffset
						(
						IMemoryPool *pmp,
						CParseHandlerManager *pphm,
						CParseHandlerBase *pphRoot
						);
		};
}
#endif // !GPDXL_CParseHandlerScalarLimitOffset_H

//EOF
