//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarCoalesce.h
//
//	@doc:
//
//		SAX parse handler class for parsing coalesce operator
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerScalarCoalesce_H
#define GPDXL_CParseHandlerScalarCoalesce_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLScalarCoalesce.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarCoalesce
	//
	//	@doc:
	//		Parse handler for parsing a coalesce operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarCoalesce : public CParseHandlerScalarOp
	{
		private:

			// return type
			IMDId *m_pmdidType;

			// private copy ctor
			CParseHandlerScalarCoalesce(const CParseHandlerScalarCoalesce &);

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
			CParseHandlerScalarCoalesce
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

		};
}

#endif // !GPDXL_CParseHandlerScalarCoalesce_H

//EOF
