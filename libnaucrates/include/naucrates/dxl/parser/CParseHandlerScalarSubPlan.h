//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSubPlan.h
//
//	@doc:
//		
//		SAX parse handler class for parsing SubPlan.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarSubPlan_H
#define GPDXL_CParseHandlerScalarSubPlan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerScalarSubPlan
	//
	//	@doc:
	//		Parse handler for parsing a scalar SubPlan
	//
	//---------------------------------------------------------------------------
	class CParseHandlerScalarSubPlan : public CParseHandlerScalarOp
	{
		private:
	
			// first col type
			IMDId *m_pmdidFirstCol;

			// subplan type
			EdxlSubPlanType m_edxlsubplantype;

			// private copy ctor
			CParseHandlerScalarSubPlan(const CParseHandlerScalarSubPlan &);
	
			// map character sequence to subplan type
			EdxlSubPlanType Edxlsubplantype(const XMLCh *xmlszSubplanType);

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
			CParseHandlerScalarSubPlan
					(
					IMemoryPool *pmp,
					CParseHandlerManager *pphm,
					CParseHandlerBase *pphRoot
					);
	};

}
#endif // GPDXL_CParseHandlerScalarSubPlan_H

//EOF
