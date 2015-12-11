//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMergeJoin.h
//
//	@doc:
//		SAX parse handler class for parsing merge join operator nodes.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMergeJoin_H
#define GPDXL_CParseHandlerMergeJoin_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalMergeJoin.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMergeJoin
	//
	//	@doc:
	//		Parse handler for merge join operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMergeJoin : public CParseHandlerPhysicalOp
	{
		private:


			// the merge join operator
			CDXLPhysicalMergeJoin *m_pdxlop;
			
			// private copy ctor
			CParseHandlerMergeJoin(const CParseHandlerMergeJoin &);
			
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
			CParseHandlerMergeJoin
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
	};
}

#endif // !GPDXL_CParseHandlerMergeJoin_H

// EOF
