//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarSubPlanTestExpr.h
//
//	@doc:
//		SAX parse handler class for parsing sub plan test expression
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarSubPlanTestExpr_H
#define GPDXL_CParseHandlerScalarSubPlanTestExpr_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarSubPlanTestExpr
	//
	//	@doc:
	//		Parse handler for parsing subplan test expression
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarSubPlanTestExpr : public CParseHandlerScalarOp
	{
		private:

			// child test expression
			CDXLNode *m_pdxlnTestExpr;

			// private copy ctor
			CParseHandlerScalarSubPlanTestExpr(const CParseHandlerScalarSubPlanTestExpr &);

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
			CParseHandlerScalarSubPlanTestExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			virtual
			~CParseHandlerScalarSubPlanTestExpr();

			// return test expression
			CDXLNode *PdxlnTestExpr() const
			{
				return m_pdxlnTestExpr;
			}
	};
}

#endif // !GPDXL_CParseHandlerScalarSubPlanTestExpr_H

// EOF
