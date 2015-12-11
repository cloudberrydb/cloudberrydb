//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarMinMax.h
//
//	@doc:
//
//		SAX parse handler class for parsing MinMax operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerScalarMinMax_H
#define GPDXL_CParseHandlerScalarMinMax_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLScalarMinMax.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarMinMax
	//
	//	@doc:
	//		Parse handler for parsing a MinMax operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarMinMax : public CParseHandlerScalarOp
	{
		private:

			// return type
			IMDId *m_pmdidType;

			// min/max type
			CDXLScalarMinMax::EdxlMinMaxType m_emmt;

			// private copy ctor
			CParseHandlerScalarMinMax(const CParseHandlerScalarMinMax &);

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

			// parse the min/max type from the attribute value
			static
			CDXLScalarMinMax::EdxlMinMaxType Emmt(const XMLCh *xmlszLocalname);

		public:
			// ctor
			CParseHandlerScalarMinMax
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

		};
}

#endif // !GPDXL_CParseHandlerScalarMinMax_H

//EOF
