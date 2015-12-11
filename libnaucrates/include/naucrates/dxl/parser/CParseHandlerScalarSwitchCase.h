//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSwitchCase.h
//
//	@doc:
//
//		SAX parse handler class for parsing a SwitchCase operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerScalarSwitchCase_H
#define GPDXL_CParseHandlerScalarSwitchCase_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLScalarSwitchCase.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarSwitchCase
	//
	//	@doc:
	//		Parse handler for parsing a SwitchCase operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarSwitchCase : public CParseHandlerScalarOp
	{
		private:

			// private copy ctor
			CParseHandlerScalarSwitchCase(const CParseHandlerScalarSwitchCase &);

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
			CParseHandlerScalarSwitchCase
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

		};
}

#endif // !GPDXL_CParseHandlerScalarSwitchCase_H

//EOF
