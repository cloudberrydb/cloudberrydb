//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBScalarOp.h
//
//	@doc:
//		SAX parse handler class for GPDB scalar operator metadata
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDGPDBScalarOp_H
#define GPDXL_CParseHandlerMDGPDBScalarOp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDGPDBScalarOp
	//
	//	@doc:
	//		Parse handler for GPDB scalar operator metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDGPDBScalarOp : public CParseHandlerMetadataObject
	{
		private:
			// id and version 
			IMDId *m_pmdid;
			
			// name
			CMDName *m_pmdname;
			
			// type of left operand
			IMDId *m_pmdidTypeLeft;
			
			// type of right operand
			IMDId *m_pmdidTypeRight;

			// type of result operand
			IMDId *m_pmdidTypeResult;
			
			// id of function which implements the operator
			IMDId *m_pmdidFunc;
			
			// id of commute operator
			IMDId *m_pmdidOpCommute;
			
			// id of inverse operator
			IMDId *m_pmdidOpInverse;
			
			// comparison type
			IMDType::ECmpType m_ecmpt;
			
			// does operator return NULL on NULL input?
			BOOL m_fReturnsNullOnNullInput;

			// private copy ctor
			CParseHandlerMDGPDBScalarOp(const CParseHandlerMDGPDBScalarOp &);
			
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

			// is this a supported child elem of the scalar op
			BOOL FSupportedChildElem(const XMLCh* const xmlsz);
						
		public:
			// ctor
			CParseHandlerMDGPDBScalarOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);			
	};
}

#endif // !GPDXL_CParseHandlerMDGPDBScalarOp_H

// EOF
