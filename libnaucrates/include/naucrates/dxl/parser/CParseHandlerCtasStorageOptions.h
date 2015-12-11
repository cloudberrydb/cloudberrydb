//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerCtasStorageOptions.h
//
//	@doc:
//		Parse handler for parsing CTAS storage options
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerCTASStorageOptions_H
#define GPDXL_CParseHandlerCTASStorageOptions_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLCtasStorageOptions.h"

#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerCtasStorageOptions
	//
	//	@doc:
	//		Parse handler for parsing CTAS storage options
	//
	//---------------------------------------------------------------------------
	class CParseHandlerCtasStorageOptions : public CParseHandlerBase
	{
		private:

			// tablespace name
			CMDName *m_pmdnameTablespace;
			
			// on commit action
			CDXLCtasStorageOptions::ECtasOnCommitAction m_ectascommit;
			
			// CTAS storage options
			CDXLCtasStorageOptions *m_pdxlctasopt;
			
			// parsed array of key-value pairs of options
			CDXLCtasStorageOptions::DrgPctasOpt *m_pdrgpctasopt;
			
			// private copy ctor
			CParseHandlerCtasStorageOptions(const CParseHandlerCtasStorageOptions &);

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
			CParseHandlerCtasStorageOptions
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// dtor
			virtual
			~CParseHandlerCtasStorageOptions();
			
			// parsed storage options
			CDXLCtasStorageOptions *Pdxlctasopt() const;
	};
}

#endif // !GPDXL_CParseHandlerCTASStorageOptions_H

// EOF
