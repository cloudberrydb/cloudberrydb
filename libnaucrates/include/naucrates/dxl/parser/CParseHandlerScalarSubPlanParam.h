//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSubPlanParam.h
//
//	@doc:
//		
//		SAX parse handler class for parsing a single Param of a SubPlan
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarSubPlanParam_H
#define GPDXL_CParseHandlerScalarSubPlanParam_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarSubPlanParam
	//
	//	@doc:
	//		Parse handler for parsing a Param of a scalar SubPlan
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarSubPlanParam : public CParseHandlerScalarOp
	{
		private:
	
			// column reference
			CDXLColRef *m_pdxlcr;

			// private copy ctor
			CParseHandlerScalarSubPlanParam(const CParseHandlerScalarSubPlanParam &);
	
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
			// ctor/dtor
			CParseHandlerScalarSubPlanParam
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

			virtual
			~CParseHandlerScalarSubPlanParam();

			// return column reference
			CDXLColRef *Pdxlcr(void)
			const
			{
				return m_pdxlcr;
			}

			// return param type
			IMDId *Pmdid(void)
			const
			{
				return m_pdxlcr->PmdidType();
			}
	};

}
#endif // GPDXL_CParseHandlerScalarSubPlanParam_H

//EOF
