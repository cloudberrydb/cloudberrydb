//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc
//
//	@filename:
//		CParseHandlerLogicalCTAS.h
//
//	@doc:
//		Parse handler for parsing a logical CTAS operator
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalCTAS_H
#define GPDXL_CParseHandlerLogicalCTAS_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalCTAS
	//
	//	@doc:
	//		Parse handler for parsing a logical CTAS operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalCTAS : public CParseHandlerLogicalOp
	{
		private:
			
			// mdid
			IMDId *m_pmdid;
			
			// schema name
			CMDName *m_pmdnameSchema;
			
			// table name
			CMDName *m_pmdname;
	
			// list of distribution column positions		
			DrgPul *m_pdrgpulDistr;
			
			// list of source column ids		
			DrgPul *m_pdrgpulSource;
			
			// list of vartypmod
			DrgPi *m_pdrgpiVarTypeMod;

			// is this a temporary table
			BOOL m_fTemporary;
			
			// does table have oids
			BOOL m_fHasOids;
			
			// distribution policy
			IMDRelation::Ereldistrpolicy m_ereldistrpolicy;
			
			// storage type
			IMDRelation::Erelstoragetype m_erelstorage;
		
			// private copy ctor
			CParseHandlerLogicalCTAS(const CParseHandlerLogicalCTAS &);

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
			CParseHandlerLogicalCTAS
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerLogicalCTAS_H

// EOF
