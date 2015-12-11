//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerSearchStrategy.h
//
//	@doc:
//		Parse handler for search strategy
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerSearchStrategy_H
#define GPDXL_CParseHandlerSearchStrategy_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "gpopt/search/CSearchStage.h"

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerSearchStrategy
	//
	//	@doc:
	//		Parse handler for search strategy
	//
	//---------------------------------------------------------------------------
	class CParseHandlerSearchStrategy : public CParseHandlerBase
	{

		private:

			// search stages
			DrgPss *m_pdrgpss;

			// private ctor
			CParseHandlerSearchStrategy(const CParseHandlerSearchStrategy&);

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
			CParseHandlerSearchStrategy
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			virtual
			~CParseHandlerSearchStrategy();

			// returns the dxl representation of search stages
			DrgPss *Pdrgppss()
			{
				return m_pdrgpss;
			}

			EDxlParseHandlerType Edxlphtype() const
			{
				return EdxlphSearchStrategy;
			}

	};
}

#endif // !GPDXL_CParseHandlerSearchStrategy_H

// EOF
