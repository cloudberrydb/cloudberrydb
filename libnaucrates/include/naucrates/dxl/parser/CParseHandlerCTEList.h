//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerCTEList.h
//
//	@doc:
//		Parse handler for parsing a CTE list
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerCTEList_H
#define GPDXL_CParseHandlerCTEList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerCTEList
	//
	//	@doc:
	//		Parse handler for parsing a CTE list
	//
	//---------------------------------------------------------------------------
	class CParseHandlerCTEList : public CParseHandlerBase
	{
		private:

			// CTE list
			DrgPdxln *m_pdrgpdxln;
			
			// private copy ctor
			CParseHandlerCTEList(const CParseHandlerCTEList &);

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
			CParseHandlerCTEList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// dtor
			virtual
			~CParseHandlerCTEList();
			
			// CTE list
			DrgPdxln *Pdrgpdxln() const
			{
				return m_pdrgpdxln;
			}
	};
}

#endif // !GPDXL_CParseHandlerCTEList_H

// EOF
