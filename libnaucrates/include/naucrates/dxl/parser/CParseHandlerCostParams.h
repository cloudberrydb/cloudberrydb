//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerCostParams.h
//
//	@doc:
//		Parse handler for cost model paerameters
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerCostParams_H
#define GPDXL_CParseHandlerCostParams_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "gpopt/cost/ICostModelParams.h"

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerCostParams
	//
	//	@doc:
	//		Parse handler for cost model parameters
	//
	//---------------------------------------------------------------------------
	class CParseHandlerCostParams : public CParseHandlerBase
	{

		private:

			// cost params
			ICostModelParams *m_pcp;

			// private ctor
			CParseHandlerCostParams(const CParseHandlerCostParams&);

			// process the start of an element
			void StartElement
				(
				const XMLCh* const xmlstrUri, 		// URI of element's namespace
 				const XMLCh* const xmlstrLocalname,	// local part of element's name
				const XMLCh* const xmlstrQname,		// element's qname
				const Attributes& attr				// element's attributes
				);

			// process the end of an element
			void EndElement
				(
				const XMLCh* const xmlstrUri, 		// URI of element's namespace
				const XMLCh* const xmlstrLocalname,	// local part of element's name
				const XMLCh* const xmlstrQname		// element's qname
				);

		public:
			// ctor/dtor
			CParseHandlerCostParams
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			virtual
			~CParseHandlerCostParams();

			// returns the dxl representation of cost parameters
			ICostModelParams *Pcp()
			{
				return m_pcp;
			}

			EDxlParseHandlerType Edxlphtype() const
			{
				return EdxlphCostParams;
			}

	};
}

#endif // !GPDXL_CParseHandlerCostParams_H

// EOF
