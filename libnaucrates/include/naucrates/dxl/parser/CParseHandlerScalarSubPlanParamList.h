//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSubPlanParamList.h
//
//	@doc:
//		
//		SAX parse handler class for parsing SubPlan ParamList
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarSubPlanParamList_H
#define GPDXL_CParseHandlerScalarSubPlanParamList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarSubPlanParamList
	//
	//	@doc:
	//		Parse handler for parsing a scalar SubPlan ParamList
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarSubPlanParamList : public CParseHandlerScalarOp
	{
		private:
	
			BOOL m_fParamList;

			// array of outer column references
			DrgPdxlcr *m_pdrgdxlcr;

			// private copy ctor
			CParseHandlerScalarSubPlanParamList(const CParseHandlerScalarSubPlanParamList &);
	
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
			CParseHandlerScalarSubPlanParamList
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);

			// dtor
			virtual
			~CParseHandlerScalarSubPlanParamList();

			// return param column references
			DrgPdxlcr *Pdrgdxlcr()
			const
			{
				return m_pdrgdxlcr;
			}
	};

}
#endif // GPDXL_CParseHandlerScalarSubPlanParamList_H

//EOF
