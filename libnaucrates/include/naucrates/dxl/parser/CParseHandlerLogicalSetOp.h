//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalSetOp.h
//
//	@doc:
//		Parse handler for parsing a logical set operator
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalSetOp_H
#define GPDXL_CParseHandlerLogicalSetOp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"
#include "naucrates/dxl/operators/CDXLLogicalSetOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalSetOp
	//
	//	@doc:
	//		Parse handler for parsing a logical set operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalSetOp : public CParseHandlerLogicalOp
	{
		private:

			// set operation type
			EdxlSetOpType m_edxlsetop;

			// array of input column id arrays
			DrgPdrgPul *m_pdrgpdrgpulInputColIds;

			// do the columns across inputs need to be casted
			BOOL m_fCastAcrossInputs;

			// private copy ctor
			CParseHandlerLogicalSetOp(const CParseHandlerLogicalSetOp &);

			// return the set operation type
			EdxlSetOpType Edxlsetop(const XMLCh* const xmlszLocalname);

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
			CParseHandlerLogicalSetOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			~CParseHandlerLogicalSetOp();
	};
}

#endif // !GPDXL_CParseHandlerLogicalSetOp_H

// EOF
