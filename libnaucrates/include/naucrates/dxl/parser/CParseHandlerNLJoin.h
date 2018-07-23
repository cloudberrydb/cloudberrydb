//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerNLJoin.h
//
//	@doc:
//		SAX parse handler class for parsing nested loop join operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerNLJoin_H
#define GPDXL_CParseHandlerNLJoin_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

#include "naucrates/dxl/operators/CDXLPhysicalNLJoin.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	// indices of nested loop join elements in the children array
	enum ENLJoinParseHandlerChildIndices
	{
		EdxlParseHandlerNLJIndexProp = 0,
		EdxlParseHandlerNLJIndexProjList,
		EdxlParseHandlerNLJIndexFilter,
		EdxlParseHandlerNLJIndexJoinFilter,
		EdxlParseHandlerNLJIndexLeftChild,
		EdxlParseHandlerNLJIndexRightChild,
		EdxlParseHandlerNLJIndexNestLoopParams,
		EdxlParseHandlerNLJIndexSentinel
	};
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerNLJoin
	//
	//	@doc:
	//		Parse handler for nested loop join operators
	//
	//---------------------------------------------------------------------------
	class CParseHandlerNLJoin : public CParseHandlerPhysicalOp
	{
		private:


			// the nested loop join operator
			CDXLPhysicalNLJoin *m_pdxlop;
			
			// private copy ctor
			CParseHandlerNLJoin(const CParseHandlerNLJoin &);

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
			CParseHandlerNLJoin
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
	};
}

#endif // !GPDXL_CParseHandlerNLJoin_H

// EOF
